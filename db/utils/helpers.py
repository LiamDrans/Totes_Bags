''' various helper functions '''
from typing import List, Dict
from .functional import compose

def format_response(columns, rows, castings=None, label='') -> List|Dict:
    ''' format the rows returned from PG8000 query '''
    col_names = [col['name'] for col in columns]

    if not castings:
        return { label: [dict(zip(col_names, row)) for row in rows] }

    formatted = []

    if isinstance(castings, List):
        cast_values = compose(*castings)

        for row in rows:
            cast_row = [cast_values(value) for value in row]
            formatted.append(dict(zip(col_names, cast_row)))

    return { label: formatted }
