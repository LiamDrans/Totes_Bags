"""Script to pytest all utils functions"""

from decimal import Decimal
from pytest import mark
from db.utils.converters import convert_datetime, convert_decimal

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
