"""Script to pytest all utils functions"""
import json
import boto3
from moto import mock_aws
from pytest import mark
from db.utils.helpers import format_response
from db.utils.json_io import save_json
from db.utils.get_bucket_names import get_data_bucket_name, check_bucket_has_files
from .sample_row import test_row, test_columns, resulting_row

@mark.it('Test format response')
def test_format_response():

    result = format_response(test_columns, test_row, label='test')
    assert result['test'] == resulting_row

@mark.it('Test JSON Encoder saves JSON and converts datetime and decimal')
def test_save_json():
    result = json.loads(save_json(resulting_row))
    assert isinstance(result[0]['created_at'], str)
    assert isinstance(result[0]['unit_price'], float)

@mark.it('Test function corretly retrieves name of the data bucket')
@mock_aws
def test_get_data_bucket_name():
    s3 = boto3.client('s3', region_name='us-east-1')
    s3.create_bucket(Bucket='test-bucket-1234124')
    response = get_data_bucket_name('test-bucket-')
    assert response == 'test-bucket-1234124'

@mark.it('Test function returns False when bucket is empty')
@mock_aws
def test_empty_bucket():
    s3 = boto3.client('s3', region_name='us-east-1')
    s3.create_bucket(Bucket='test-bucket-1234124')
    response = check_bucket_has_files('test-bucket-1234124')
    assert response is False

@mark.it('Test function returns True when bucket has contents')
@mock_aws
def test_full_bucket():
    s3 = boto3.client('s3', region_name='us-east-1')
    s3.create_bucket(Bucket='test-bucket-1234124')
    test_data = [1, 2, 3, 4]
    s3.put_object(Body=json.dumps(test_data), Bucket='test-bucket-1234124',Key='test.json')
    response = check_bucket_has_files('test-bucket-1234124')
    assert response is True
