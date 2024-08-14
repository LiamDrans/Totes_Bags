from os import environ as env
from pg8000.native import Connection, Error
from dotenv import load_dotenv
load_dotenv()

class CreateConnection():
    def __init__(self):
        self.connection = None

    def __enter__(self):
        try:
            self.connection = Connection(
                env.get('USERNAME'),
                host=env.get('HOSTNAME'),
                password=env.get('PASSWORD'),
                database=env.get('DATABASE'),
                port=env.get('PORT')
            )
            return self.connection
        except Error as e:
            return str(e)

    def __exit__(self, exc_type, exc_val, exc_tb):
        print('closing connection...')
        if isinstance(self.connection, Connection):
            self.connection.close()
            self.connection = None

if __name__ == '__main__':
    with CreateConnection() as conn:
        print(conn)
