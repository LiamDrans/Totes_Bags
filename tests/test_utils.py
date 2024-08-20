"""Script to pytest all utils functions"""
from datetime import datetime
from unittest.mock import patch
import json
from pytest import mark, raises
from src.extract.utils.helpers import format_response, prepend_time
from src.extract.utils.json import json_encode
from .sample_row import test_row, test_columns, resulting_row

@mark.it('Test format response')
def test_format_response():
    result = format_response(test_columns, test_row, label='test')
    assert result['test'] == resulting_row


@mark.it("Test raises typerror 'int' object is not iterable")
def test_raises_typerror():
    with raises(TypeError) as excinfo:
        format_response(1, [], 'test')
    assert "'int' object is not iterable" in str(excinfo.value)


@mark.it('Test prepends date to string')
@patch('src.extract.utils.helpers.datetime')
def test_prepend_string(date):
    date.now.return_value = datetime(2000,1,1)
    assert prepend_time('_my_file.json') == '2000/January/01/00:00:00_my_file.json'


@mark.it('Test JSON Encoder converts to valid JSON with datetime and decimal')
def test_json_encoder():
    result = json.loads(json_encode(resulting_row))
    assert isinstance(result[0]['created_at'], str)
    assert isinstance(result[0]['unit_price'], float)

# TODO: possibly should be able to take other iterable types
@mark.it('Test JSON Encoder raises a typeerror if argument is not dictionary')
def test_json_encoder_typeerror():
    with raises(TypeError) as excinfo:
        json_encode('abc')
    assert 'argument needs to be a dictionary' in str(excinfo.value)