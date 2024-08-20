"""Script for lambda_handler"""

from db.db_crud_functions import fetch_all_tables
from db.s3_crud_functions import get_bucket_name, bucket_file_count, upload_to_bucket
from db.utils.json import json_encode
from db.utils.helpers import prepend_time

def lambda_handler(event, context) -> None:
    """lambda function to upload totes table data in json format to S3 Bucket"""
    bucket_name = get_bucket_name('totes-data-')
    is_update = bucket_file_count(bucket_name)
    totes_tables = fetch_all_tables(updates=is_update)

    if not totes_tables:
        print('No new updates to be added to the data bucket')
        return

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

if __name__ == '__main__':
    lambda_handler(event=True, context=True)
    # with open('./db/json_files/zzz_db_totesys.json', 'w', encoding='utf-8') as f:
    #     f.write(save_json(fetch_all_tables()))
