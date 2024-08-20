''' Converter functions '''
import json
from typing import List, Dict, Union
from decimal import Decimal
from datetime import datetime

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


def json_encode(data: Union[Dict, List]) -> None:
    ''' saves json file to destination '''
    if not isinstance(data, (dict, list)):
        raise TypeError('argument needs to be a dictionary')

    return json.dumps(data, separators=(',', ':'), cls=CustomJSONEncoder)
