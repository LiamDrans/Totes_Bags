"""these functions will get the bucket list and return the bucket"""
import boto3


def get_data_bucket_name():
    """getting the data bucket name"""
    s3 = boto3.client('s3', region_name='eu-west-2')

    response = s3.list_buckets()
    bucket_prefix = "totes-data-"

    try:
        if not response['Buckets']:
            raise ValueError("No buckets found in S3")

        for bucket in response['Buckets']:
            if bucket['Name'].startswith(bucket_prefix):
                return bucket['Name']
        raise ValueError(f"No bucket found with prefix: {bucket_prefix}")
    except ValueError as e:
        print(f"An error occurred while accessing S3: {e}")
        raise

def get_processed_bucket_name():
    """getting the data bucket name"""
    s3 = boto3.client('s3', region_name='eu-west-2')

    response = s3.list_buckets()
    bucket_prefix = "totes-processed-data"

    try:
        if not response['Buckets']:
            raise ValueError("No buckets found in S3")

        for bucket in response['Buckets']:
            if bucket['Name'].startswith(bucket_prefix):
                return bucket['Name']
        raise ValueError(f"No bucket found with prefix: {bucket_prefix}")
    except ValueError as e:
        print(f"An error occurred while accessing S3: {e}")
        raise

