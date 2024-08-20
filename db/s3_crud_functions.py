'''Crud functions for S3 bucket operations'''
import logging
from typing import Dict
import boto3
from botocore.exceptions import ClientError

# Set up our logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

def get_bucket_name(bucket_prefix: str) -> str:
    """gets the bucket name by prefix
    Args:
        bucket_prefix (str): prefix for bucket name
    Returns:
        str: bucket name
    """
    try:
        s3 = boto3.resource('s3')
        bucket_list = s3.buckets.all()

        if not bucket_list:
            raise ValueError('No buckets found in S3')

        for bucket in bucket_list:
            if bucket.name.startswith(bucket_prefix):
                return bucket.name
        raise ValueError(f'No bucket found with prefix: {bucket_prefix}')

    except ClientError as err:
        logger.error('An error occurred while accessing S3: %s', err)
        raise err


def get_bucket_file_count(bucket_name: str) -> int:
    """get file count for bucket
    Args:
        bucket_name (str): bucket to count files in.
    Returns:
        int: file count
    """
    try:
        s3 = boto3.client('s3')
        response = s3.list_objects_v2(Bucket=bucket_name)
        return len(response.get('Contents', []))
    except ClientError as err:
        logger.error('An error occurred while accessing S3: %s', err)
        raise err


def bucket_has_file(bucket_name: str, file_name: str) -> bool:
    """check if file in bucket exists
    Args:
        bucket_name (str): bucket to check
        file_name (str): file to check for
    Returns:
        bool: True if file exists, False otherwise
    """
    try:
        s3_client = boto3.client('s3')
        s3_client.get_object(Bucket=bucket_name, Key=file_name)
        return True
    except s3_client.exceptions.NoSuchKey:
        return False

    except ClientError as err:
        logger.error('An error occurred while accessing S3: %s', err)
        raise err


def upload_to_bucket(obj: Dict) -> None:
    """upload object to bucket
    Args:
        obj (Dict): object to upload
        e.g. {'Body': 'data', 'Bucket': 'bucket_name', 'Key': 'file_name'}
    """
    try:
        s3_client = boto3.client('s3')
        s3_client.put_object(**obj)
    except ClientError as err:
        logger.error('An error occurred while uploading to S3: %s', err)
        raise err


if __name__ == '__main__':
    totes_bucket = get_bucket_name('totes-data-')
    print(get_bucket_file_count(totes_bucket))
    print(bucket_has_file(totes_bucket, 'latest_db_totes.json'))
