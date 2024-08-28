import boto3
import pandas as pd
from .utils.get_bucket_names import get_data_bucket_name, get_processed_bucket_name
import json
from copy import deepcopy
from datetime import datetime


def pull_latest_json_from_data_bucket():
    bucket_name = get_data_bucket_name()
    file_name = "latest_db_totes.json"
    s3 = boto3.client("s3")
    file = s3.get_object(Bucket=bucket_name, Key=file_name)
    data = file["Body"].read().decode('utf-8')
    data_list = json.loads(data)
    table_names = []
    for table in data_list:
        for key in table:
            table_names.append(key)
    return (data_list, table_names)


def format_list_to_dict_of_dataframes(data_list, table_name):
    df_dict = {}
    for i in range(0, len(table_name)):
        temp_dict = data_list[i]
        df_dict[table_name[i]] = pd.DataFrame(temp_dict[table_name[i]])
    return df_dict


def dim_design(df_old):
    df = deepcopy(df_old)
    df.sort_values(by="design_id", inplace=True)
    df.reset_index(inplace=True)
    df.drop(columns=['created_at', 'last_updated', 'index'], inplace=True)
    return df


def fact_payment(df_old):
    df = deepcopy(df_old)
    # splitting date time columns
    df['created_date_1'] = df['created_at'].str.slice(stop=16)
    df['created_date'] = df['created_date_1'].apply(lambda x: change_date_format(x))
    df['created_time'] = df['created_at'].str.slice(start=17)
    df['last_updated_date_1'] = df['last_updated'].str.slice(stop=16)
    df['last_updated_date'] = df['last_updated_date_1'].apply(lambda x: change_date_format(x))
    df['last_updated_time'] = df['last_updated'].str.slice(start=17)

    # sort and drop columns
    df.sort_values(by="payment_id", inplace=True)
    df.reset_index(inplace=True)
    df.drop(
        columns=['created_at', 'last_updated', 'index', 'company_ac_number', 'counterparty_ac_number', 'created_date_1',
                 'created_date_1'], inplace=True)
    df.index.name = 'payment_record_id'
    df.reset_index(inplace=True)

    # rearrange column order
    df = df[['payment_record_id',
             'payment_id',
             'created_date',
             'created_time',
             'last_updated_date',
             'last_updated_time',
             'transaction_id',
             'counterparty_id',
             'payment_amount',
             'currency_id',
             'payment_type_id',
             'paid',
             'payment_date'
             ]]

    # print(df)
    return df


def fact_sales_order(df_old):
    df = deepcopy(df_old)
    # splitting date time columns
    df['created_date_1'] = df['created_at'].str.slice(stop=16)
    df['created_date'] = df['created_date_1'].apply(lambda x: change_date_format(x))
    df['created_time'] = df['created_at'].str.slice(start=17)
    df['last_updated_date_1'] = df['last_updated'].str.slice(stop=16)
    df['last_updated_date'] = df['last_updated_date_1'].apply(lambda x: change_date_format(x))
    df['last_updated_time'] = df['last_updated'].str.slice(start=17)

    # sort and drop columns
    df.sort_values(by="sales_order_id", inplace=True)
    df.reset_index(inplace=True)
    df.rename(columns={'staff_id': 'sales_staff_id'}, inplace=True)
    df.drop(columns=['created_at', 'last_updated', 'index', 'created_date_1', 'created_date_1'], inplace=True)
    df.index.name = 'sales_record_id'
    df.reset_index(inplace=True)

    # rearrange column order
    df = df[['sales_record_id',
             'sales_order_id',
             'created_date',
             'created_time',
             'last_updated_date',
             'last_updated_time',
             'sales_staff_id',
             'counterparty_id',
             'units_sold',
             'unit_price',
             'currency_id',
             'design_id',
             'agreed_payment_date',
             'agreed_delivery_date',
             'agreed_delivery_location_id'
             ]]

    return df


def change_date_format(input_date):
    result = datetime.strptime(input_date, "%a, %d %b %Y").strftime("%Y-%m-%d")
    return result


def fact_purchase_order(df_old):
    df = deepcopy(df_old)
    # splitting date time columns
    df['created_date_1'] = df['created_at'].str.slice(stop=16)
    df['created_date'] = df['created_date_1'].apply(lambda x: change_date_format(x))
    df['created_time'] = df['created_at'].str.slice(start=17)
    df['last_updated_date_1'] = df['last_updated'].str.slice(stop=16)
    df['last_updated_date'] = df['last_updated_date_1'].apply(lambda x: change_date_format(x))
    df['last_updated_time'] = df['last_updated'].str.slice(start=17)

    # sort and drop columns
    df.sort_values(by="purchase_order_id", inplace=True)
    df.reset_index(inplace=True)
    df.drop(columns=['created_at', 'last_updated', 'index', 'created_date_1', 'created_date_1'], inplace=True)
    df.index.name = 'purchase_record_id'
    df.reset_index(inplace=True)

    # rearrange column order
    df = df[['purchase_record_id',
             'purchase_order_id',
             'created_date',
             'created_time',
             'last_updated_date',
             'last_updated_time',
             'staff_id',
             'counterparty_id',
             'item_code',
             'item_quantity',
             'item_unit_price',
             'currency_id',
             'agreed_delivery_date',
             'agreed_payment_date',
             'agreed_delivery_location_id'
             ]]
    return df


def dim_location(df_old):
    df = deepcopy(df_old)
    # sort and drop columns
    df.sort_values(by="address_id", inplace=True)
    df.reset_index(inplace=True)
    df.drop(columns=['created_at', 'last_updated', 'index'], inplace=True)
    df.rename(columns={'address_id': 'location_id'}, inplace=True)
    return df


def dim_currency(df_old):
    df = deepcopy(df_old)
    # sort and drop columns
    conversion_dict = {"GBP": "Pound Sterling",
                       "USD": "United States Dollar",
                       "EUR": "Euro"}

    df.sort_values(by="currency_id", inplace=True)
    df.reset_index(inplace=True)
    df.drop(columns=['created_at', 'last_updated', 'index'], inplace=True)
    df['currency_name'] = df['currency_code'].map(conversion_dict)
    return df


def dim_staff(df_staff_old, df_department_old):
    df_staff = deepcopy(df_staff_old)
    df_department = deepcopy(df_department_old)
    df = df_staff.merge(df_department, on='department_id', how='left')
    df.drop(columns=['created_at_y', 'last_updated_y', 'created_at_x', 'last_updated_x', 'manager', 'department_id'],
            inplace=True)

    df = df[["staff_id",
             "first_name",
             "last_name",
             "department_name",
             "location",
             "email_address"
             ]]
    return df


def dim_counterparty(df_counterparty_old, df_address_old):
    df_counterparty = deepcopy(df_counterparty_old)
    df_address = deepcopy(df_address_old)
    df = pd.merge(df_counterparty, df_address, left_on='legal_address_id', right_on='address_id', how='left')

    df.drop(columns=['created_at_y', 'last_updated_y', 'created_at_x', 'last_updated_x',
                     'address_id', 'commercial_contact', 'delivery_contact', 'legal_address_id'], inplace=True)

    df.rename(columns={'address_line_1': 'counterparty_legal_address_line_1',
                       'address_line_2': 'counterparty_legal_address_line_2',
                       'distric': 'counterparty_legal_district',
                       'city': 'counterparty_legal_city',
                       'postal_code': 'counterparty_legal_postal_code',
                       'country': 'counterparty_legal_country',
                       'phone': 'counterparty_legal_phone_number'
                       }, inplace=True)
    return df


def dim_date(start='2020-01-01', end='2025-12-31'):
    df = pd.DataFrame({"date_id": pd.date_range(start, end)})
    df["year"] = df.date_id.dt.year
    df["month"] = df.date_id.dt.month
    df["day"] = df.date_id.dt.day
    df["day_of_week"] = df.date_id.dt.day_of_week
    df["day_name"] = df.date_id.dt.day_name()
    df["month_name"] = df.date_id.dt.month_name()
    df["quarter"] = df.date_id.dt.quarter
    return df


def dim_payment_type(df_old):
    df = deepcopy(df_old)
    df.sort_values(by="payment_type_id", inplace=True)
    df.reset_index(inplace=True)
    df.drop(columns=['created_at', 'last_updated', 'index'], inplace=True)
    # pprint(df)
    return df


def dim_transaction(df_old):
    df = deepcopy(df_old)
    df.sort_values(by="transaction_id", inplace=True)
    df.reset_index(inplace=True)
    df.drop(columns=['created_at', 'last_updated', 'index'], inplace=True)
    return df


def transform_all_tables(df_old):
    df_new = {}
    # Need no table
    df_new["dim_date"] = dim_date()
    # Needs one table
    if "purchase_order" in df_old:
        df_new["fact_purchase_order"] = fact_purchase_order(df_old["purchase_order"])
    if "payment" in df_old:
        df_new["fact_payment"] = fact_payment(df_old["payment"])
    if "sales_order" in df_old:
        df_new["fact_sales_order"] = fact_sales_order(df_old["sales_order"])
    if "currency" in df_old:
        df_new["dim_currency"] = dim_currency(df_old["currency"])
    if "design" in df_old:
        df_new["dim_design"] = dim_design(df_old["design"])
    if "address" in df_old:
        df_new["dim_location"] = dim_location(df_old["address"])
    if "transaction" in df_old:
        df_new["dim_transaction"] = dim_transaction(df_old["transaction"])
    if "payment_type" in df_old:
        df_new["dim_payment_type"] = dim_payment_type(df_old["payment_type"])
    # needs two tables
    if "counterparty" in df_old and "address" in df_old:
        df_new["dim_counterparty"] = dim_counterparty(df_old["counterparty"], df_old["address"])
    if "staff" in df_old and "department" in df_old:
        df_new["dim_staff"] = dim_staff(df_old["staff"], df_old["department"])
    return df_new

def prepend_time():
    """prepends current time to given string"""
    return datetime.now().strftime("%Y/%B/%d/%H:%M/")