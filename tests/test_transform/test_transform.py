from unittest.mock import patch
import pandas as pd
import boto3
from moto import mock_aws

from src.transform.totes_star_schema import pull_latest_json_from_data_bucket, transform_all_tables
from src.transform.transform import lambda_handler
from src.transform.utils.get_bucket_names import get_data_bucket_name

@mock_aws
@patch('src.transform.transform.transform_all_tables')
@patch('src.transform.transform.format_list_to_dict_of_dataframes')
@patch('src.transform.transform.pull_latest_json_from_data_bucket')
def test_lambda_handler_saves_data_to_s3(mock_pull_latest_json_from_data_bucket, mock_format_list_to_dict_of_dataframes, mock_transform_all_tables):
    """Test lambda_handler saves to S3."""
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='totes-processed-data-1234124', CreateBucketConfiguration={
        'LocationConstraint': 'eu-west-2'})
    data = {
        'Column1': [1, 2, 3, 4],
        'Column2': ['A', 'B', 'C', 'D']
    }
    df = pd.DataFrame(data)

    mock_transform_all_tables.return_value = {}
    mock_transform_all_tables.return_value['mock_data'] = df
    mock_format_list_to_dict_of_dataframes.return_value = {}
    mock_pull_latest_json_from_data_bucket.return_value = ([], [])

    lambda_handler()

    response = s3.list_objects(Bucket='totes-processed-data-1234124')
    assert response is not None




