from pytest import mark
from src.extract.db.connection import CreateConnection


@mark.it('dummy connection test')
def test_create_connection():
    with CreateConnection() as conn:
        print(conn)
    assert True
