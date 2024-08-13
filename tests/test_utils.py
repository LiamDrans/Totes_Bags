"""Script to pytest all utils functions"""

from decimal import Decimal
from datetime import datetime
from pytest import mark
from db.utils.converters import convert_datetime, convert_decimal
from db.utils.functional import compose

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
