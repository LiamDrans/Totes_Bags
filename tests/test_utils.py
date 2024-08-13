"""Script to pytest all utils functions"""

from decimal import Decimal
from datetime import datetime
from pytest import mark
from db.utils.converters import convert_datetime, convert_decimal
from db.utils.functional import compose
from db.utils.helpers import format_response
from .sample_row import test_row, test_columns, resulting_row

@mark.describe('testing data converter functions')
class TestConverters:

    @mark.it('Decimal is converted to a float')
    def test_convert_decimal(self):
        result = convert_decimal(Decimal(2.42))
        assert isinstance(result, float)
    
    @mark.it('Non-decimal remains unchanged')
    def test_convert_non_decimal(self):
        result = convert_decimal(2)
        assert isinstance(result, int)

    @mark.it('Datetime is converted to a string')
    def test_convert_datetime(self):
        current_date = datetime.now()
        result = convert_datetime(current_date)
        assert isinstance(result, str)
        assert current_date.isoformat() == result
    
    @mark.it('Non-datetime remains unchanged')
    def test_convert_non_datetime(self):
        result = convert_datetime(120)
        assert isinstance(result, int)

@mark.describe('testing functional functions')
class TestFunctionals:

    @mark.it('Test compose applies functions correctly')
    def test_compose(self):
        def increment(x):
            return x + 1
        def multiply(x):
            return x*2
        calculation = compose(increment, multiply)
        assert calculation(2) == 5

@mark.describe('testing helper functions')
class TestHelpers:

    @mark.it('Test format response with decimal and datetime castings')
    def test_format_response(self):
        castings = [convert_datetime, convert_decimal]
        result = format_response(test_columns, test_row, castings, label='test')
        assert result['test'] == resulting_row
    
    @mark.it('Test format response with only decimal casting')
    def test_format_response_decimal(self):
        result = format_response(test_columns, test_row, castings=[convert_decimal], label='test')
        assert result['test'] != resulting_row
        assert isinstance(result['test'][0]['unit_price'], float)
        assert isinstance(result['test'][0]['last_updated'], datetime)

    @mark.it('Test format response with only datetime casting')
    def test_format_response_datetime(self):
        result = format_response(test_columns, test_row, castings=[convert_datetime], label='test')
        assert result['test'] != resulting_row
        assert isinstance(result['test'][0]['unit_price'], Decimal)
        assert isinstance(result['test'][0]['last_updated'], str)

    @mark.it('Test format response with no casting')
    def test_format_response_no_castings(self):
        result = format_response(test_columns, test_row, castings=None, label='test')
        assert result['test'] != resulting_row
        assert isinstance(result['test'][0]['unit_price'], Decimal)
        assert isinstance(result['test'][0]['last_updated'], datetime)
