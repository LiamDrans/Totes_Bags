""" initial crud operations for the database """

from typing import Optional, Union, Dict, List, Tuple
from pg8000.native import Connection, Error, identifier
from .connection import CreateConnection
from ..utils.helpers import format_response


def query_db(
    sql: str, /,
    placeholders: Optional[Dict] = None,
    conn: Optional[Connection] = None,
    types: Optional[Dict] = None
) -> Union[List, None]:
    """Query the database and return the result as a list, or None if no rows are returned.
    Args:
        sql (str): query string,
        placeholders (Dict): optional query placeholder parameters,
        conn (Connection): optional database connection
        types (Dict): optional dictionary of types to cast the results to

    Returns:
        List: a list of rows from the database or None if no rows are returned
    """
    if isinstance(conn, Connection):
        return conn.run(sql, **(placeholders or {}), types=types)

    with CreateConnection() as new_conn:
        return new_conn.run(sql, **(placeholders or {}), types=types)


def fetch_table_rows(
    table_name: str, /,
    last_time_queried: Optional[str] = None,
    conn: Optional[Connection] = None
) -> Union[Dict, bool]:
    """fetches table data and formats it
    Args:
        table_name (str): name of the table to query
        last_time_queried (str): optional time to query from
        conn (Connection): the database connection
        last_time_checked (str): The timestamp of the last time the function was called,
        formatted as 'YYYY-MM-DD HH:MI:SS'.

    Returns:
        Dict: table data formatted into a dictionary or False if no data is returned
    """
    sql = f"SELECT * FROM {identifier(table_name)};"
    placeholders = {}

    if last_time_queried:
        sql = f"""
            SELECT *
            FROM {identifier(table_name)}
            WHERE last_updated >= :last_time_queried;
        """
        placeholders = {'last_time_queried': last_time_queried}

    if rows := query_db(sql, placeholders=placeholders, conn=conn):
        return format_response(conn.columns, rows, label=table_name)

    return False


def fetch_table_names(conn: Optional[Connection] = None) -> Union[List, bool]:
    """fetches all public table names from database"""

    sql = """
        SELECT table_name FROM information_schema.tables
        WHERE table_schema='public' AND table_name ~ '^[a-z]'
    """

    if rows:= query_db(sql, conn=conn):
        return [row[0] for row in rows]
    return False


def fetch_all_tables(last_time_queried: Optional[str] = None) -> Tuple[str, List]:
    """fetches data from all the tables
    Args:
        last_time_queried (str): optional time to query from
    Returns:
        Dict: Dictionary with the last time queried and a list of tables with their data
    """
    with CreateConnection() as conn:
        try:
            return (
                query_db('SELECT NOW()', conn=conn)[0][0].isoformat(),
                [
                    row for name in fetch_table_names(conn)
                    if (row:= fetch_table_rows(
                        name,
                        last_time_queried=last_time_queried,
                        conn=conn
                    ))
                ]
            )
        except Error as err:
            print(str(err))
            raise
