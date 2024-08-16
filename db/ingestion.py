"""Script for lambda_handler"""

import datetime
import boto3
from crud_functions import save_all_tables
from utils.get_bucket_names import get_data_bucket_name

def lambda_handler():

    """lambda function to put zip on S3 Bucket"""

    save_all_tables()


    bucket_name = get_data_bucket_name()

    s3 = boto3.client('s3', region_name='eu-west-2')
    
    s3.upload_file(
        'db/json_files/db_totes.zip',
        Bucket=bucket_name,
        Key=f'{datetime.datetime.now()}_db_totes.zip'
    )

    s3.upload_file(
        'db/json_files/db_totes.zip',
        Bucket=bucket_name,
        Key='latest_db_totes.zip'
    )

if __name__ == '__main__':
    lambda_handler()