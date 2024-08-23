from pytest import mark, raises
from src.extract.app.db.connection import CreateConnection
from src.extract.app.db.db_credentials import get_db_credentials


@mark.it('dummy connection test')
def test_create_connection():
    with CreateConnection() as conn:
        print(conn)
    assert True

def test_db_get_credentials_returns_exception_with_invalid_secret_name():
    with raises(Exception):
        get_db_credentials('invalid_secret_name')