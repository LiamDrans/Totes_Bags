import unittest
from unittest.mock import patch

import boto3
from moto import mock_aws

from src.transform.totes_star_schema import pull_latest_json_from_data_bucket
from src.transform.transform import lambda_handler
from src.transform.utils.get_bucket_names import get_data_bucket_name

@mock_aws
@patch('src.transform.totes_star_schema.pull_latest_json_from_data_bucket')
@patch('src.transform.totes_star_schema.format_list_to_dict_of_dataframes')
@patch('src.transform.totes_star_schema.transform_all_tables')
def test_lambda_handler_saves_data_to_s3(mock_transform_all_tables, mock_format_list_to_dict_of_dataframes, mock_pull_latest_json_from_data_bucket):
    """Test lambda_handler saves to S3."""
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='totes-data-1234124', CreateBucketConfiguration={
        'LocationConstraint': 'eu-west-2'})
    mock_transform_all_tables.return_value = None

    lambda_handler()

    response = s3.list_objects(Bucket='totes-processed-data-1234124')
    assert response == 'totes-data-1234124'



if __name__ == '__main__':
    unittest.main()
