import unittest

import boto3
from moto import mock_aws

from src.transform.utils.get_bucket_names import get_data_bucket_name, get_processed_bucket_name


@mock_aws
def test_get_bucket_name():
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='totes-data-1234124', CreateBucketConfiguration={
        'LocationConstraint': 'eu-west-2'})
    response = get_data_bucket_name()
    assert response == 'totes-data-1234124'

@mock_aws
def test_get_bucket_name_without_buckets_returns_appropriate_error():
    try:
        get_data_bucket_name()
    except ValueError as e:
        assert str(e) == "No buckets found in S3"

@mock_aws
def test_get_bucket_name_without_prefix_returns_appropriate_error():
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='test-bucket-1234124', CreateBucketConfiguration={
        'LocationConstraint': 'eu-west-2'})
    try:
        get_data_bucket_name()
    except ValueError as e:
        assert str(e) == "No bucket found with prefix: totes-data-"

@mock_aws
def test_get_processed_bucket_name_returns_appropriate_bucket():
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='totes-processed-data-1234124', CreateBucketConfiguration={
        'LocationConstraint': 'eu-west-2'})
    response = get_processed_bucket_name()
    assert response == 'totes-processed-data-1234124'

if __name__ == '__main__':
    unittest.main()
