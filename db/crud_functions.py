''' initial crud operations for the database '''

import json
from typing import Dict, List
from connection import CreateConnection
from pg8000.native import identifier, Error
from utils.helpers import format_response
from utils.converters import convert_datetime, convert_decimal


def fetch_one_table(table_name: str) -> Dict:
    """ fetches table data and formats it into JSON formattable data

    Arguments:
        table_name (str): name of the table to query
        conn: the database connection

    Returns:
        dict: table data formatted into a dictionary
    """
    with CreateConnection() as conn:
        rows = conn.run(f'SELECT * FROM {identifier(table_name)};')
        print(rows[0])

        if conn.row_count:

            return format_response(
                conn.columns,
                rows,
                label=table_name,
                castings=[convert_datetime, convert_decimal]
            )
        return False


def fetch_table_names() -> List:
    ''' fetches all public table names from database '''
    sql = """
    SELECT
        table_name
    FROM information_schema.tables
    WHERE table_schema='public' AND table_name ~ '^[a-z]'
    """

    with CreateConnection() as conn:
        rows = conn.run(sql)
        if conn.row_count:
            return [row[0] for row in rows]
        return False


def fetch_all_tables() -> List:
    ''' fetches data from all the tables '''
    try:
        table_names = fetch_table_names()
        return [fetch_one_table(name) for name in table_names]
    except Error as e:
        print(str(e))


if __name__ == '__main__':
    with open('db_totes.json', 'w', encoding='utf-8') as f:
        f.write(json.dumps(fetch_all_tables(), indent=2))
