from totes_star_schema import format_list_to_dict_of_dataframes, pull_latest_json_from_data_bucket, \
     transform_all_tables, get_processed_bucket_name, prepend_time



"""template for future"""
def lambda_handler():
    """template for future"""    

    (data_list, table_names) = pull_latest_json_from_data_bucket()
    df_old=format_list_to_dict_of_dataframes(data_list, table_names)
    df_new=transform_all_tables(df_old)

    bucket_name=get_processed_bucket_name()
    folder_name = prepend_time()    
    
    for key in df_new:
        df_new[key].to_parquet(f's3://{bucket_name}/{folder_name}{key}.gzip', compression='gzip')
        df_new[key].to_parquet(f's3://{bucket_name}/latest_updates/{key}.gzip', compression='gzip')  
    

    return

lambda_handler()