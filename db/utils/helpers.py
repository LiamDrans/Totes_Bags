''' various helper functions '''
from typing import List, Dict

def format_response(columns: List, rows: List, label: str) -> Dict:
    ''' format the rows returned from PG8000 query '''
    col_names = [col['name'] for col in columns]
    return { label: [dict(zip(col_names, row)) for row in rows] }
