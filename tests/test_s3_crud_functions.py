"""Script to pytest all utils functions"""
import json
import boto3
from botocore.exceptions import ParamValidationError
from moto import mock_aws
from pytest import mark, raises
from src.extract.app.s3_crud_functions import (
    get_bucket_name,
    get_object_head,
    upload_to_bucket
)

@mark.describe('Test putting and retrieving data from S3 buckets')
class TestBucketCrudOperations:
    @mark.it('Test function corretly retrieves name of the data bucket')
    @mock_aws
    def test_get_data_bucket_name(self):
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='test-bucket-1234124')

        assert get_bucket_name('test-bucket-') == 'test-bucket-1234124'


    @mark.it(
        'Test function returns boolean depending on whether specified file is in bucket'
    )
    @mock_aws
    def test_has_file_in_bucket(self):
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='test-bucket-1234124')

        assert not get_object_head('test-bucket-1234124', 'test.json')

        s3.put_object(
            Body=json.dumps([1, 2, 3]),
            Bucket='test-bucket-1234124',
            Key='test.json'
        )

        assert get_object_head('test-bucket-1234124', 'test.json')


    @mark.it('Test file is correctly added to the bucket')
    @mock_aws
    def test_adding_file_to_bucket(self):
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='test-bucket-1234124')

        upload_to_bucket(
            {
                'Body': json.dumps([1, 2, 3]),
                'Bucket':'test-bucket-1234124',
                'Key': 'test.json'
            }
        )

        response = s3.get_object(Bucket='test-bucket-1234124', Key='test.json')
        assert json.load(response['Body']) == [1, 2, 3]


@mark.describe('Test error handling of S3 crud functions')
class TestCrudFunctionErrorHandling:


    @mark.it("Test excepts a ValueError with 'No bucket found in S3'")
    @mock_aws
    def test_get_bucket_name_no_buckets(self):
        boto3.client('s3', region_name='us-east-1')

        with raises(ValueError) as excinfo:
            get_bucket_name('nonsense_name')
        assert 'No buckets found in S3' in str(excinfo.value)


    @mark.it("Test excepts a ValueError with 'No bucket found with prefix'")
    @mock_aws
    def test_get_bucket_name_no_such_bucket(self):
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='test-bucket-1234124')

        with raises(ValueError) as excinfo:
            get_bucket_name('nonsense_name')
        assert 'No bucket found with prefix' in str(excinfo.value)


    @mark.it('Test raises ParamValidationError when provided with incorrect keys')
    @mock_aws
    def test_adding_file_to_bucket_wrong_keys(self):
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='test-bucket-1234124')

        with raises(ParamValidationError) as excinfo:
            upload_to_bucket(
                {
                    'Bdy': json.dumps([1, 2, 3]),
                    'Bucket':'test-bucket-1234124',
                    'Key': 'test.json'
                }
            )
        assert 'Parameter validation failed' in str(excinfo.value)
