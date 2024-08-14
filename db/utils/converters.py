''' Converter functions '''
import json
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

# Custom class extends json.JSONEncoder
class CustomJSONEncoder(json.JSONEncoder):
    ''' Custom JSON Encoder '''
    def default(self, o):

        if isinstance(o, datetime):
            return o.strftime('%a, %d %b %Y %H:%M:%S GMT')

        if isinstance(o, Decimal):
            return float(o)

        # Default behavior for all other types
        return super().default(o)
