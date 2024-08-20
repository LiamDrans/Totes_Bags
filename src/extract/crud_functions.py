''' initial crud operations for the database '''
from typing import Optional, Union, Dict, List
from pg8000.native import Connection, identifier, Error
from connection import CreateConnection
from utils.helpers import format_response

def query_db(sql: str, conn: Optional[Connection] = None) -> Union[List, None]:
    """Query the database and return the result as a list, or None if no rows are returned.
    Arguments:
        sql (str): query string
        conn: the database connection

    Returns:
        List: a list of rows from the database or None if no rows are returned
    """
    if not isinstance(conn, Connection):
        with CreateConnection() as new_conn:
            return new_conn.run(sql)
    return conn.run(sql)


def fetch_one_table(table_name: str, conn: Optional[Connection] = None) -> Union[Dict, bool]:
    """ fetches table data and formats it
    Arguments:
        table_name (str): name of the table to query
        conn: the database connection

    Returns:
        Dict: table data formatted into a dictionary or False if no data is returned
    """
    if rows:= query_db(f'SELECT * FROM {identifier(table_name)};', conn):
        return format_response(conn.columns, rows, label=table_name)
    return False


def fetch_table_names(conn: Optional[Connection] = None) -> Union[List, bool]:
    ''' fetches all public table names from database '''
    sql = """SELECT table_name FROM information_schema.tables
             WHERE table_schema='public' AND table_name ~ '^[a-z]'
    """
    if (rows:= query_db(sql, conn)):
        return [row[0] for row in rows]
    return False


def fetch_all_tables() -> List|bool:
    ''' fetches data from all the tables '''
    with CreateConnection() as conn:
        try:
            table_names = fetch_table_names(conn)
            return_list = []

            for name in table_names:
                table_data = fetch_one_table(name, conn)
                return_list.append(table_data)
                return return_list

        except Error as e:
            print(str(e))
        return 'return' #added for pylint

if __name__ == '__main__':
    fetch_all_tables()
