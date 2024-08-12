from typing import Union, List, Dict

def format_response(columns, rows, label:str = '') -> Union[List, Dict]:
    ''' format the rows returned from PG8000 query '''
    col_names = [col['name'] for col in columns]
    formatted = []

    if rows:
        formatted = [dict(zip(col_names, row)) for row in rows]

    return { label: formatted } if label else formatted