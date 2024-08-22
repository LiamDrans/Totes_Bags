from totes_star_schema import format_list_to_dict_of_dataframes, pull_latest_json_from_data_bucket, \
     fact_payment, fact_purchase_order, fact_sales_order, dim_currency, dim_date,   \
        dim_design, dim_location, dim_staff, dim_counterparty, dim_payment_type, dim_transaction
from pprint import pprint



"""template for future"""
def lambda_handler():
    """template for future"""    

    (data_list, table_names) = pull_latest_json_from_data_bucket()
    df_old=format_list_to_dict_of_dataframes(data_list, table_names)
    df_new={}
    
    #Need no table
    df_new["dim_date"]=dim_date()
    
    #Needs one table
    if "purchase_order" in df_old:
        df_new["fact_purchase_order"]=fact_purchase_order(df_old["purchase_order"])
    if "payment" in df_old:
        df_new["fact_payment"]=fact_payment(df_old["payment"])
    if "sales_order" in df_old:
        df_new["fact_sales_order"]=fact_sales_order(df_old["sales_order"])
    if "currency" in df_old:
        df_new["dim_currency"]=dim_currency(df_old["currency"])
    if "design" in df_old:
        df_new["dim_design"]=dim_design(df_old["design"])
    if "address" in df_old:
        df_new["dim_location"]=dim_location(df_old["address"])
    if "transaction" in df_old:
        df_new["dim_transaction"]=dim_transaction(df_old["transaction"])
    if "payment_type" in df_old:
        df_new["dim_payment_type"]=dim_payment_type(df_old["payment_type"])

    # needs two tables
    if "counterparty" in df_old and "address" in df_old:
        df_new["dim_counterparty"]=dim_counterparty(df_old["counterparty"],df_old["address"])
    if "staff" in df_old and "department" in df_old:
        df_new["dim_staff"]=dim_staff(df_old["staff"],df_old["department"])

    return

lambda_handler()
