''' various helper functions '''
import logging
from typing import List, Dict

def format_response(columns: List, rows: List, label: str) -> Dict:
    ''' format the rows returned from PG8000 query '''

    try:
        col_names = [col['name'] for col in columns]
        return { label: [dict(zip(col_names, row)) for row in rows] }
    except ValueError as e:
        logging.error(f"An error occurred while running format_response: {e}")
        raise e

    return 'return' #added for pylint
