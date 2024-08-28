"""Test suite for Load Lambda Handler"""
import gzip
import json
import io
import os
from unittest.mock import patch, MagicMock
from moto import mock_aws
import boto3
import pytest
from load.app.load import lambda_handler

SAMPLE_SECRET = {
    "cohort_id": "test",
    "user": "test",
    "password": "test",
    "host": "test",
    "database": "test",
    "port": 5432,
    "schema": "test"
}
SAMPLE_EVENT = {
    "Records": [
        {
            "s3": {
                "bucket": {"name": "my-test-bucket"},
                "object": {"key": "fact_sales_order.gzip"}
            }
        }
    ]
}

SAMPLE_CONTENT = "column1,column2\nvalue1,value2\nvalue3,value4"

@pytest.fixture(scope='function')
def s3_and_secrets():
    """Mock S3 and Secrets Manager"""
    with mock_aws():
        s3_client = boto3.client("s3", region_name="eu-west-2")
        s3_client.create_bucket(
            Bucket="my-test-bucket",
            CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
        )
        
        secrets_client = boto3.client("secretsmanager", region_name="eu-west-2")
        secrets_client.create_secret(Name="DataWarehouse", SecretString=json.dumps(SAMPLE_SECRET))
        
        yield s3_client, secrets_client


@patch('pg8000.connect')
def test_lambda_handler(mock_connect, s3_and_secrets):
    """Actual test for Load Lambda Handler"""
    s3_client, _ = s3_and_secrets

    with io.BytesIO() as fileobj:
        with gzip.GzipFile(fileobj=fileobj, mode="wb") as gz:
            gz.write(SAMPLE_CONTENT.encode('utf-8'))
        fileobj.seek(0)
        s3_client.put_object(
            Bucket="my-test-bucket", Key="fact_sales_order.gzip", Body=fileobj.getvalue()
            )

    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection

    with patch.dict(os.environ, {"SECRET_NAME": "DataWarehouse"}):
        response = lambda_handler(SAMPLE_EVENT, None)
        assert response["statusCode"] == 200
        mock_connection.cursor().execute.assert_called()

@patch('src.load.load.process_gzip_file')
@patch('src.load.load.connect_to_db')
@patch('src.load.load.get_secret')
def test_lambda_handler_with_get_secret_error(
    mock_get_secret, mock_connect_to_db, mock_process_gzip_file, s3_and_secrets
    ):
    """Test lambda_handler when get_secret raises an exception."""
    s3_client, _ = s3_and_secrets
    mock_get_secret.side_effect = Exception("Secret retrieval error")

    with pytest.raises(Exception):
        lambda_handler(SAMPLE_EVENT, None)

@patch('src.load.load.process_gzip_file')
@patch('src.load.load.connect_to_db')
@patch('src.load.load.get_secret')
def test_lambda_handler_with_db_connection_error(
    mock_get_secret, mock_connect_to_db, mock_process_gzip_file, s3_and_secrets
    ):
    """Test lambda_handler when connect_to_db raises an exception."""
    s3_client, _ = s3_and_secrets
    mock_get_secret.return_value = SAMPLE_SECRET
    mock_connect_to_db.side_effect = Exception("DB connection error")

    with pytest.raises(Exception):
        lambda_handler(SAMPLE_EVENT, None)

@patch('src.load.load.process_gzip_file')
@patch('src.load.load.connect_to_db')
@patch('src.load.load.get_secret')
def test_lambda_handler_with_process_gzip_file_error(
    mock_get_secret, mock_connect_to_db, mock_process_gzip_file, s3_and_secrets
    ):
    """Test lambda_handler when process_gzip_file raises an exception."""
    s3_client, _ = s3_and_secrets
    mock_get_secret.return_value = SAMPLE_SECRET
    mock_process_gzip_file.side_effect = Exception("Processing error")

    with pytest.raises(Exception):
        lambda_handler(SAMPLE_EVENT, None)
