''' Converter functions '''
from typing import Any
from decimal import Decimal
from datetime import datetime

def convert_decimal(value: Any) -> float|Any:
    ''' convert Decimals to float '''
    if isinstance(value, Decimal):
        return float(value)
    return value

def convert_datetime(value: Any) -> str|Any:
    ''' convert datetimes to string '''
    if isinstance(value, datetime):
        return value.isoformat()
    return value
