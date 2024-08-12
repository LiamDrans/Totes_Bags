import json
from pprint import pprint
from connection import CreateConnection
from pg8000.native import identifier
from utils.helpers import format_response
from utils.converters import convert_datetime, convert_decimal

def fetch_table(table_name: str) -> dict:
    ''' fetches table by table name formatting the output '''
    with CreateConnection() as conn:
        rows = conn.run(f'SELECT * FROM {identifier(table_name)} LIMIT 5')
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
    # pprint(sales_orders, indent=2)
    print(json.dumps(sales_orders, indent=2))
