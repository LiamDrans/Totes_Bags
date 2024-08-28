"""A collection of helper functions"""
import json
import gzip
import io
import logging
import pg8000
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def get_secret(secret_name):
    """A function that gets secrets from AWS for our lambda function"""
    secrets_client = boto3.client('secretsmanager')
    response = secrets_client.get_secret_value(SecretId=secret_name)
    secret_string = response['SecretString']
    return json.loads(secret_string)

def connect_to_db(credentials):
    """Function that handles connection"""
    try:
        connection = pg8000.connect(
            user=credentials['user'],
            password=credentials['password'],
            host=credentials['host'],
            port=credentials['port'],
            database=credentials['database'],
        )
        return connection
    except Exception as e:
        logger.error(f"Error connecting to the database: {e}")
        raise

def process_gzip_file(connection, bucket_name, object_key, credentials):
    """Function that gets files from Processed s3 Bucket and uses gzip
        to bring it into the insert function"""
    s3_client = boto3.client('s3')
    try:
        s3_object = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        with gzip.GzipFile(fileobj=io.BytesIO(s3_object['Body'].read())) as gzipfile:
            content = gzipfile.read().decode('utf-8')
        
        table_name = determine_table_name(object_key)
        
        if table_name:
            logger.info(f"Inserting data into table {table_name}")
            insert_data_into_db(connection, content, credentials['schema'], table_name)
        else:
            logger.warning(f"Unrecognized file {object_key}. Skipping.")

    except Exception as e:
        logger.error(f"Error processing file {object_key} from bucket {bucket_name}: {e}")
        raise

def determine_table_name(object_key):
    """Helper function within the helper function. Assists process_gzip_file
        with finding the name of the table to be used in insert_data_into_db"""
    file_table_map = {
        "fact_sales_order.gzip": "fact_sales_order",
        "fact_purchase_orders.gzip": "fact_purchase_orders",
        "fact_payment.gzip": "fact_payment",
        "dim_transaction.gzip": "dim_transaction",
        "dim_staff.gzip": "dim_staff",
        "dim_payment_type.gzip": "dim_payment_type",
        "dim_location.gzip": "dim_location",
        "dim_design.gzip": "dim_design",
        "dim_date.gzip": "dim_date",
        "dim_currency.gzip": "dim_currency",
        "dim_counterparty.gzip": "dim_counterparty"
    }
    return file_table_map.get(object_key)

def insert_data_into_db(connection, content, schema, table_name):
    """Function for inserting to our Data Warehouse"""
    cursor = connection.cursor()
    try:
        
        table_insert_queries = {
            "dim_date": f"INSERT INTO {schema}.dim_date (date_id, year, month, day, day_of_week, day_name, month_name, quarter) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            "dim_staff": f"INSERT INTO {schema}.dim_staff (staff_id, first_name, last_name, department_name, location, email_address) VALUES (%s, %s, %s, %s, %s, %s)",
            "dim_location": f"INSERT INTO {schema}.dim_location (location_id, address_line_1, address_line_2, district, city, postal_code, country, phone) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            "dim_currency": f"INSERT INTO {schema}.dim_currency (currency_id, currency_code, currency_name) VALUES (%s, %s, %s)",
            "dim_design": f"INSERT INTO {schema}.dim_design (design_id, design_name, file_location, file_name) VALUES (%s, %s, %s, %s)",
            "dim_counterparty": f"INSERT INTO {schema}.dim_counterparty (counterparty_id, counterparty_legal_name, counterparty_legal_address_line_1, counterparty_legal_address_line_2, counterparty_legal_district, counterparty_legal_city, counterparty_legal_country, counterparty_legal_postal_code, counterparty_legal_phone_number) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
            "dim_payment_type": f"INSERT INTO {schema}.dim_payment_type (payment_type_id, payment_type_name) VALUES (%s, %s)",
            "dim_transaction": f"INSERT INTO {schema}.dim_transaction (transaction_id, transaction_type, sales_order_id, purchase_order_id) VALUES (%s, %s, %s, %s)",
            "fact_sales_order": f"INSERT INTO {schema}.fact_sales_order (sales_record_id, created_date, created_time, last_updated_date, last_updated_time, sale_staff_id, counterparty_id, units_sold, unit_price, currency_id, design_id, agreed_payment_date, agreed_delivery_date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            "fact_purchase_order": f"INSERT INTO {schema}.fact_purchase_order (purchase_record_id, created_date, created_time, last_updated_date, last_updated_time, staff_id, counterparty_id, item_code, item_quantity, item_unit_price, currency_id, agreed_delivery_date, agreed_payment_date, agreed_delivery_location_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            "fact_payment": f"INSERT INTO {schema}.fact_payment (payment_record_id, created_date, created_time, last_updated_date, last_updated_time, transaction_id, counterparty_id, payment_amount, currency_id, payment_type_id, paid, payment_date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        }
         
        insert_query = table_insert_queries.get(table_name)
        if not insert_query:
            logger.warning(f"No insert query found for table {table_name}. Skipping.")
            return
        
        data = content.splitlines()
        for row in data:
            values = row.split(',')
            cursor.execute(insert_query, values)
        
        connection.commit()
        logger.info(f"Data successfully inserted into table {table_name}")
    except Exception as e:
        logger.error(f"Error inserting data into table {table_name}: {e}")
        connection.rollback()
        raise
    finally:
        cursor.close()