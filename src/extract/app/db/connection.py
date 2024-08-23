"""context manager connection class for totesys database"""
import logging
from pg8000.native import Connection, Error
from .db_credentials import get_db_credentials


class CreateConnection:
    """class for connection using credentials from AWS Secrets Manager"""

    def __init__(self) -> None:
        self.connection = None

    def __enter__(self):
        try:
            secret = get_db_credentials('totesys_db')

            self.connection = Connection(
                secret['USERNAME'],
                host=secret['HOSTNAME'],
                password=secret['PASSWORD'],
                database=secret['DATABASE'],
                port=secret['PORT']
            )
            return self.connection
        except Error as e:
            logging.error(f'Error connecting to database totesys_db: {e}')
            return str(e)

    def __exit__(self, exc_type, exc_val, exc_tb):
        print('closing connection...')
        if isinstance(self.connection, Connection):
            self.connection.close()
            self.connection = None
