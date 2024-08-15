''' initial crud operations for the database '''
import time
from zipfile import ZipFile, ZIP_DEFLATED
from typing import Optional, Union, Dict, List
from db.connection import CreateConnection
from pg8000.native import Connection, identifier, Error
from db.utils.json_io import save_json
from db.utils.helpers import format_response


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
    if (rows:= query_db(f'SELECT * FROM {identifier(table_name)};', conn)):
        return format_response(conn.columns, rows, label=table_name)
    return False


def fetch_table_names(conn: Optional[Connection] = None) -> Union[List, bool]:
    ''' fetches all public table names from database '''
    sql = """
        SELECT
            table_name
        FROM
            information_schema.tables
        WHERE table_schema='public' AND table_name ~ '^[a-z]'
    """
    if (rows:= query_db(sql, conn)):
        return [row[0] for row in rows]
    return False


def save_all_tables() -> List|bool:
    ''' fetches data from all the tables '''
    with CreateConnection() as conn:
        try:
            table_names = fetch_table_names(conn)

            for name in table_names:
                table_data = fetch_one_table(name, conn)
                filename = f'./db/json_files/db_totes_{name}.json'

                save_json(table_data, filename)
            
            time.sleep(3)

            with ZipFile('./db/json_files/db_totes.zip', 'w', ZIP_DEFLATED, compresslevel=9) as z:
                for name in table_names:
                    z.write(f'./db/json_files/db_totes_{name}.json')

        except Error as e:
            print(str(e))

if __name__ == '__main__':
    save_all_tables()
