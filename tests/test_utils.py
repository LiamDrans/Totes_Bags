"""Script to pytest all utils functions"""
# TODO: refactor for JSONEncoder
from decimal import Decimal
from datetime import datetime
from pytest import mark
from db.utils.helpers import format_response
from .sample_row import test_row, test_columns, resulting_row

@mark.describe('testing helper functions')
class TestHelpers:

    @mark.it('Test format response with decimal and datetime castings')
    def test_format_response(self):

        result = format_response(test_columns, test_row, label='test')
        assert result['test'] == resulting_row

    @mark.it('Test format response with only decimal casting')
    def test_format_response_decimal(self):
        result = format_response(test_columns, test_row, label='test')
        assert result['test'] != resulting_row
        assert isinstance(result['test'][0]['unit_price'], float)
        assert isinstance(result['test'][0]['last_updated'], datetime)

    @mark.it('Test format response with only datetime casting')
    def test_format_response_datetime(self):
        result = format_response(test_columns, test_row, label='test')
        assert result['test'] != resulting_row
        assert isinstance(result['test'][0]['unit_price'], Decimal)
        assert isinstance(result['test'][0]['last_updated'], str)

    @mark.it('Test format response with no casting')
    def test_format_response_no_castings(self):
        result = format_response(test_columns, test_row, label='test')
        assert result['test'] != resulting_row
        assert isinstance(result['test'][0]['unit_price'], Decimal)
        assert isinstance(result['test'][0]['last_updated'], datetime)
