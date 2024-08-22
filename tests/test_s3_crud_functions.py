"""Script to pytest all utils functions"""
import json
import boto3
from botocore.exceptions import ClientError
from moto import mock_aws
from pytest import mark, raises
from src.extract.app.s3_crud_functions import (
    get_bucket_name,
    get_bucket_file_count,
    bucket_has_file,
    upload_to_bucket
)

@mark.describe('Test putting and retrieving data from S3 buckets')
class TestBucketCrudOperations:
    @mark.it('Test function corretly retrieves name of the data bucket')
    @mock_aws
    def test_get_data_bucket_name(self):
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='test-bucket-1234124')
        response = get_bucket_name('test-bucket-')
        assert response == 'test-bucket-1234124'


    @mark.it('Test function returns False when bucket is empty')
    @mock_aws
    def test_empty_bucket(self):
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='test-bucket-1234124')
        response = get_bucket_file_count('test-bucket-1234124')
        assert response == 0


    @mark.it(
        'Test function returns boolean depending on whether specified file is in bucket'
    )
    @mock_aws
    def test_has_file_in_bucket(self):
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='test-bucket-1234124')

        assert not bucket_has_file('test-bucket-1234124', 'test.json')

        test_data = [1, 2, 3, 4]
        s3.put_object(Body=json.dumps(test_data), Bucket='test-bucket-1234124', Key='test.json')

        assert bucket_has_file('test-bucket-1234124', 'test.json')


    @mark.it('Test function returns True when bucket has contents')
    @mock_aws
    def test_full_bucket(self):
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='test-bucket-1234124')
        test_data = [1, 2, 3, 4]
        s3.put_object(Body=json.dumps(test_data), Bucket='test-bucket-1234124',Key='test.json')
        response = get_bucket_file_count('test-bucket-1234124')
        assert response > 0


    @mark.it('Test file is correctly added to the bucket')
    @mock_aws
    def test_adding_file_to_bucket(self):
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='test-bucket-1234124')

        test_data = [1, 2, 3, 4]
        upload_to_bucket(
            {'Body': json.dumps(test_data), 'Bucket':'test-bucket-1234124', 'Key': 'test.json'}
        )

        response = s3.get_object(Bucket='test-bucket-1234124', Key='test.json')
        assert json.load(response['Body']) == [1, 2, 3, 4]


@mark.describe('Test error handling of S3 crud functions')
class TestCrudFunctionErrorHandling:

    @mark.it("Test excepts a ClientError with 'The specified bucket does not exist'")
    @mock_aws
    def test_get_bucket_file_count_no_bucket(self):
        boto3.client('s3', region_name='us-east-1')

        with raises(ClientError) as excinfo:
            get_bucket_file_count('nonsense_name')
        assert 'The specified bucket does not exist' in str(excinfo.value)


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


    @mark.it("Test excepts a ClientError with 'The specified bucket does not exist'")
    @mock_aws
    def test_bucket_has_file_no_such_bucket(self):
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket='test-bucket-1234124')

        with raises(ClientError) as excinfo:
            bucket_has_file('nonsense_bucket', 'nonsense_name')
        assert 'The specified bucket does not exist' in str(excinfo.value)
