''' Converter functions '''
import json
from typing import Dict
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

def save_json(data: Dict) -> None:
    ''' saves json file to destination '''
    try:
        return json.dumps(data, separators=(',', ':'), cls=CustomJSONEncoder)
    except ValueError as e:
        print(f"An error occurred while running save_json: {e}")
    return 'return' #added for pylint
