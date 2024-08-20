"""Script to pytest all utils functions"""
import json
import os

import pytest
from pytest import mark
from db.utils.helpers import format_response
from db.utils.json_io import save_json
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


# TODO: Implement tests for get_data_bucket_name, use MOTO