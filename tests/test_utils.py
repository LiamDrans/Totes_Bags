"""Script to pytest all utils functions"""
import json
import os
from pytest import mark
from db.utils.helpers import format_response
from db.utils.json_io import save_json
from .sample_row import test_row, test_columns, resulting_row


@mark.describe('testing helper functions')
class TestHelpers:

    @mark.it('Test format response')
    def test_format_response(self):

        result = format_response(test_columns, test_row, label='test')
        assert result['test'] == resulting_row

@mark.describe('testing JSON Encoder')
class TestJSON:

    @mark.it('Test JSON Encoder saves JSON and converts datetime and decimal')
    def test_save_json(self):
        save_json(resulting_row, 'tests/test_save_json.json')
        with open('tests/test_save_json.json', 'r', encoding='utf-8') as f:
            result = json.load(f)
            assert isinstance(result[0]['created_at'], str)
            assert isinstance(result[0]['unit_price'], float)
        os.remove("tests/test_save_json.json")

class TestGetDataBucketName:

    @mark.it('Test ')
    def test_save_json(self):
        pass
    
    #use MOTO