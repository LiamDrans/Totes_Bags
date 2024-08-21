""" lambda_handler for handling uploading table data to S3 Bucket """

from .db.db_crud_functions import fetch_all_tables
from .s3_crud_functions import get_bucket_name, get_bucket_file_count, upload_to_bucket
from .utils.json import json_encode
from .utils.helpers import prepend_time

# pylint: disable=unused-argument
def lambda_handler(event, context) -> None:
    """lambda function to upload totes table data in json format to S3 Bucket"""
    bucket_name = get_bucket_name('totes-data-')
    is_update = get_bucket_file_count(bucket_name)
    totes_tables = fetch_all_tables(updates=is_update)

    if not totes_tables:
        print('No new updates to be added to the data bucket')
        return False

    totes_tables_json = json_encode(totes_tables)

    upload_to_bucket({
        'Body': totes_tables_json,
        'Bucket': bucket_name,
        'Key': prepend_time('_db_totes.json')
    })

    upload_to_bucket({
        'Body': totes_tables_json,
        'Bucket': bucket_name,
        'Key': 'latest_db_totes.json' if is_update else 'full_db_totes.json'
    })

    return True
