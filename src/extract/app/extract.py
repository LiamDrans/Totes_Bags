""" lambda_handler for handling uploading table data to S3 Bucket """

from .db.db_crud_functions import fetch_all_tables
from .s3_crud_functions import get_bucket_name, get_object_head, upload_to_bucket
from .utils.json import json_encode
from .utils.helpers import prepend_time

# pylint: disable=unused-argument
def lambda_handler(event, context) -> None:
    """lambda function to upload totes table data in json format to S3 Bucket"""

    bucket_name = get_bucket_name('totes-data-')
    last_time_queried = None

    # get the last time the database was queried from file's metadata
    if latest_db_totes_meta:= get_object_head(bucket_name, 'latest_db_totes.json'):
        last_time_queried = latest_db_totes_meta['Metadata']['last_time_queried']

    # fetch all rows from tables from the last time queried
    latest_time_queried, tables = fetch_all_tables(last_time_queried)

    if not tables:
        print('No new updates to be added to the data bucket')
        return False

    tables_json = json_encode(tables)

    upload_to_bucket({
        'Body': tables_json,
        'Bucket': bucket_name,
        'Key': prepend_time('_db_totes.json')
    })

    upload_to_bucket({
        'Body': tables_json,
        'Bucket': bucket_name,
        'Key': 'latest_db_totes.json',
        'Metadata': {
            'last_time_queried': latest_time_queried
        }
    })

    return True
