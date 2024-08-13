''' initial crud operations for the database '''

import json
from typing import Dict
from connection import CreateConnection
from pg8000.native import identifier
from utils.helpers import format_response
from utils.converters import convert_datetime, convert_decimal

def fetch_table(table_name: str, conn: CreateConnection=None) -> Dict:
    """ fetches table data and formats it into JSON formattable data

    Arguments:
        table_name (str): name of the table to query
        conn: the database connection

    Returns:
        dict: table data formatted into a dictionary
    """
    with CreateConnection() as conn:
        rows = conn.run(f'SELECT * FROM {identifier(table_name)};')

        if conn.row_count:
            return format_response(
                conn.columns,
                rows,
                label=table_name,
                castings=[convert_datetime, convert_decimal]
            )
        return False

if __name__ == '__main__':
    sales_orders = fetch_table('sales_order')
    print(json.dumps(sales_orders, indent=2))
