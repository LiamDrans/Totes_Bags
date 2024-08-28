"""Test suite that tests our helper functions found in src/load/helpers.py"""
import io
import gzip
import json
from unittest.mock import patch, MagicMock
import pytest
from pytest import mark
import boto3
from moto import mock_aws
from load.app.helpers import get_secret, connect_to_db, process_gzip_file, determine_table_name

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

@mark.it('Test get_secret function grabs secrets')
def test_get_secret(s3_and_secrets):
    """Tests our get_secret function grabs test secrets from our client"""
    _, secrets_client = s3_and_secrets

    
    secret = get_secret("DataWarehouse")
    assert secret["user"] == "test"
    assert secret["password"] == "test"


@patch('pg8000.connect')
def test_connect_to_db(mock_connect, s3_and_secrets):
    """Tests connect_to_db function behaves correctly when interacting
        with pg8000.connect without making an actual connection"""
    mock_connect.return_value = MagicMock()  
    connection = connect_to_db(SAMPLE_SECRET)
    assert connection is not None
    mock_connect.assert_called_once_with(
        user=SAMPLE_SECRET["user"],
        password=SAMPLE_SECRET["password"],
        host=SAMPLE_SECRET["host"],
        port=SAMPLE_SECRET["port"],
        database=SAMPLE_SECRET["database"]
    )

@patch('pg8000.connect')
def test_connect_to_db_error(mock_connect):
    """Tests connect_to_db's error handling"""
    mock_connect.side_effect = Exception("Connection failed")
    
    with pytest.raises(Exception, match="Connection failed"):
        connect_to_db(SAMPLE_SECRET)

@patch('pg8000.connect')
def test_process_gzip_file(mock_connect, s3_and_secrets):
    """Tests and mocks process_gzip_file is called and can handle an s3 object"""
    s3_client, _ = s3_and_secrets
    
    
    with io.BytesIO() as fileobj:
        with gzip.GzipFile(fileobj=fileobj, mode="wb") as gz:
            gz.write(SAMPLE_CONTENT.encode('utf-8'))
        fileobj.seek(0)
        s3_client.put_object(Bucket="my-test-bucket", Key="fact_sales_order.gzip", Body=fileobj.getvalue())

    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection

    
    process_gzip_file(mock_connection, "my-test-bucket", "fact_sales_order.gzip", SAMPLE_SECRET)
    mock_connection.cursor().execute.assert_called()

@patch('pg8000.connect')
@patch('boto3.client')
def test_process_gzip_file_s3_error(mock_boto_client, mock_connect):
    """Tests process_gzip_file error handling in the result of an s3 error"""
    mock_boto_client.side_effect = Exception("S3 Error")
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection

    with pytest.raises(Exception, match="S3 Error"):
        process_gzip_file(mock_connection, "my-test-bucket", "fact_sales_order.gzip", SAMPLE_SECRET)

def test_table_names():
    """Tests our determine_table_name will return expected results"""
    expected = {
        "fact_sales_order.gzip": "fact_sales_order",
        "fact_purchase_orders.gzip": "fact_purchase_orders",
        "fact_payment.gzip": "fact_payment",
        "dim_transaction.gzip": "dim_transaction",
        "dim_staff.gzip": "dim_staff",
        "dim_payment_type.gzip": "dim_payment_type",
        "dim_location.gzip": "dim_location",
        "dim_design.gzip": "dim_design",
        "dim_date.gzip": "dim_date",
        "dim_currency.gzip": "dim_currency",
        "dim_counterparty.gzip": "dim_counterparty"
    }
    
    for object_key, expected_table_name in expected.items():
        assert determine_table_name(object_key) == expected_table_name