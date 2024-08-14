"""Script for lambda_handler"""

import datetime
import boto3
from db.crud_functions import save_all_tables
from db.utils.get_bucket_names import get_data_bucket_name

def lambda_handler(event, context):

    """lambda function to put zip on S3 Bucket"""

    save_all_tables()


    bucket_name = get_data_bucket_name()

    s3 = boto3.resource('s3')
    
    s3.put_object(
        Body='db/json_files/db_totes.zip',
        Bucket=bucket_name,
        Key=f'{datetime.datetime.now()}_db_totes.zip'
    )

    s3.put_object(
        Body='db/json_files/db_totes.zip',
        Bucket=bucket_name,
        Key='latest_db_totes.zip'
    )

