'''Crud functions for S3 bucket operations'''
import logging
from typing import Union, Dict
import boto3
from botocore.exceptions import ClientError, ParamValidationError

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

        if not list(bucket_list):
            logger.error('No buckets found in S3')
            raise ValueError('No buckets found in S3')

        for bucket in bucket_list:
            if bucket.name.startswith(bucket_prefix):
                return bucket.name

        logger.error('No bucket found with prefix: %s', bucket_prefix)
        raise ValueError(f'No bucket found with prefix: {bucket_prefix}')

    except ClientError as err:
        logger.error(err)
        raise err


def get_object_head(bucket_name: str, file_name: str) -> Union[Dict, bool]:
    """If file exists, returns the head object metadata
    Args:
        bucket_name (str): bucket to check
        file_name (str): file to check for
    Returns:
        Dict: head object metadata
        bool: False if file does not exist
    """
    try:
        s3_client = boto3.client('s3')
        return s3_client.head_object(Bucket=bucket_name, Key=file_name)
    except ClientError as err:
        logger.error(err)
        return False


def upload_to_bucket(obj: Dict) -> None:
    """upload object to bucket
    Args:
        obj (Dict): object to upload
        e.g. {'Body': 'data', 'Bucket': 'bucket_name', 'Key': 'file_name'}
    """
    try:
        s3_client = boto3.client('s3')
        s3_client.put_object(**obj)
    except (ClientError, ParamValidationError) as err:
        logger.error(err)
        raise err
