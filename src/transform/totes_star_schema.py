import boto3
import pandas as pd
from pprint import pprint
from utils.get_bucket_names import get_data_bucket_name
import io
import json

def pull_latest_json_from_data_bucket():    
    # bucket_name = get_data_bucket_name()
    bucket_name = "temp-test-ismail"
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
    df_dict={}
    for i in range(0,len(table_name)):
        temp_dict=data_list[i]
        df_dict[table_name[i]]=pd.DataFrame(temp_dict[table_name[i]]) 
    return df_dict


def dim_design(df):       
    df.sort_values(by="design_id", inplace=True)
    df.reset_index(inplace=True)
    df.drop(columns=['created_at', 'last_updated', 'index'], inplace=True)
    pprint(df)
    return df

def fact_payment(df):
    #splitting date time columns    
    df['created_date']=df['created_at'].str.slice(stop=16)
    df['created_time']=df['created_at'].str.slice(start=17)
    df['last_updated_date']=df['last_updated'].str.slice(stop=16)
    df['last_updated_time']=df['last_updated'].str.slice(start=17)

    #sort and drop columns
    df.sort_values(by="payment_id", inplace=True)
    df.reset_index(inplace=True)
    df.drop(columns=['created_at', 'last_updated','index','company_ac_number','counterparty_ac_number'], inplace=True)
    df.index.name='payment_record_id'
    df.reset_index(inplace=True)

    # rearrage column order
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

    print(df)
    return df

def fact_sales_order(df):
    #splitting date time columns    
    df['created_date']=df['created_at'].str.slice(stop=16)
    df['created_time']=df['created_at'].str.slice(start=17)
    df['last_updated_date']=df['last_updated'].str.slice(stop=16)
    df['last_updated_time']=df['last_updated'].str.slice(start=17)

    #sort and drop columns
    df.sort_values(by="sales_order_id", inplace=True)
    df.reset_index(inplace=True)
    df.rename(columns={'staff_id':'sales_staff_id'},inplace=True)
    df.drop(columns=['created_at', 'last_updated','index'], inplace=True)
    df.index.name='sales_record_id'
    df.reset_index(inplace=True)

    # rearrage column order
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

    pprint(df)
    return


def fact_purchase_order(df):
    #splitting date time columns    
    df['created_date']=df['created_at'].str.slice(stop=16)
    df['created_time']=df['created_at'].str.slice(start=17)
    df['last_updated_date']=df['last_updated'].str.slice(stop=16)
    df['last_updated_time']=df['last_updated'].str.slice(start=17)

    #sort and drop columns
    df.sort_values(by="purchase_order_id", inplace=True)
    df.reset_index(inplace=True)
    df.drop(columns=['created_at', 'last_updated','index'], inplace=True)
    df.index.name='purchase_record_id'
    df.reset_index(inplace=True)

    # rearrage column order
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

    print(df)
    return df


def dim_location(df):   
    #sort and drop columns
    df.sort_values(by="address_id", inplace=True)
    df.reset_index(inplace=True)
    df.drop(columns=['created_at', 'last_updated','index'], inplace=True)  
    df.rename(columns={'address_id':'location_id'},inplace=True) 

    print(df)
    return df

def dim_currency(df):   
    #sort and drop columns
    conversion_dict={"GBP": "Pound Sterling",
                     "USD": "United States Dollar",
                     "EUR": "Euro"}

    df.sort_values(by="currency_id", inplace=True)
    df.reset_index(inplace=True)
    df.drop(columns=['created_at', 'last_updated','index'], inplace=True)      
    df['currency_name'] = df['currency_code'].map(conversion_dict)
    print(df)
    return df

def dim_staff(df_staff,df_department): 
    df=df_staff.merge(df_department, on='department_id', how='left')
    df.drop(columns=['created_at_y', 'last_updated_y','created_at_x', 'last_updated_x','manager','department_id'], inplace=True)

    df=df[["staff_id",
           "first_name",
           "last_name",
           "department_name",
           "location",
           "email_address"
    ]]  
    print(df)
    return df

def dim_counterparty(df_counterparty,df_address): 
    df = pd.merge(df_counterparty, df_address, left_on='legal_address_id', right_on='address_id', how='left')
   
    df.drop(columns=['created_at_y', 'last_updated_y','created_at_x', 'last_updated_x',
                     'address_id','commercial_contact','delivery_contact', 'legal_address_id'], inplace=True)
    
    df.rename(columns={'address_line_1':'counterparty_legal_address_line_1',
                       'address_line_2':'counterparty_legal_address_line_2',
                       'distric':'counterparty_legal_district',
                       'city':'counterparty_legal_city',
                       'postal_code':'counterparty_legal_postal_code',
                       'country':'counterparty_legal_country',
                       'phone':'counterparty_legal_phone_number'
                       }, inplace=True) 


    print(df)
    return df




(data_list, table_names) = pull_latest_json_from_data_bucket()
df_old=format_list_to_dict_of_dataframes(data_list, table_names)
dim_counterparty(df_old["counterparty"], df_old["address"])





    
