""" various helper functions """
from typing import List, Dict
from datetime import datetime

def format_response(columns: List, rows: List, label: str) -> Dict:
    """ format the rows returned from PG8000 query """
    try:
        col_names = [col['name'] for col in columns]
        return { label: [dict(zip(col_names, row)) for row in rows] }
    except ValueError as e:
        print(f"An error occurred while running format_response: {e}")


def prepend_time(string: str='') -> str:
    """prepends current time to given string"""
    return datetime.now().strftime("%Y/%B/%d/%H:%M:%S") + string
