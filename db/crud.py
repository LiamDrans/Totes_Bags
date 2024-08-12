from connection import CreateConnection
from utils.utils import format_response

def fetch_table(table_name):

    with CreateConnection() as conn:

        query_string = f"""SELECT * FROM :table_name;"""

        rows = conn.run(query_string, table_name=table_name)
        if conn.row_count:
            return format_response(conn.columns, rows, table_name)
        return False

print(fetch_table("sales_order"))