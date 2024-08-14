import boto3
import datetime
from crud_functions import save_all_tables

def lambda_handler(event, context):

    """lambda function to put zip on S3 Bucket"""

    save_all_tables()

    s3 = boto3.resource('s3')

    s3.put_object(
        Body='db/json_files/db_totes.zip',
        Bucket='TBD',
        Key=f'{datetime.datetime.now()}_db_totes.zip'
    )

    s3.put_object(
        Body='db/json_files/db_totes.zip',
        Bucket='TBD',
        Key='latest_db_totes.zip'
    )

    
