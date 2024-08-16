"""test script for the db functions"""
import pytest
from pytest_postgresql import factories
from unittest.mock import MagicMock, patch
from pg8000.native import Connection
import json
import datetime
from pytest import mark
from db.connection import CreateConnection
from db.crud_functions import query_db, fetch_one_table, fetch_table_names, save_all_tables

postgresql_my_proc = factories.postgresql_proc(port=9876)
postgresql = factories.postgresql('postgresql_my_proc')


@mark.it('Test connection to database and pulls one row')
@patch('db.crud_functions.CreateConnection')
def test_query_db_returns_all_data_with_appropriate_query(MockConnection, postgresql):
    cur = postgresql.cursor()
    cur.execute("CREATE TABLE test (id serial PRIMARY KEY, num integer, data varchar);")
    cur.execute("INSERT INTO test (num, data) VALUES (1, 'test data');")
    postgresql.commit()
    cur.close()

    MockConnection.return_value = Connection(
        postgresql.pgconn.user,
        host=postgresql.pgconn.host,
        password=postgresql.pgconn.password,
        database=postgresql.pgconn.db,
        port=postgresql.pgconn.port
    )

    sql = "SELECT * FROM test LIMIT 1;"
    result = query_db(sql, MockConnection)
    assert MockConnection.called
    assert result == [[1, 1, 'test data']]


@mark.describe('testing fetch_one_table')
def test_fetch_one_table_returns_only_one_table():
    with CreateConnection() as conn:
        result = fetch_one_table('currency', conn)
        assert result == {'currency': [
            {'currency_id': 1, 'currency_code': 'GBP', 'created_at': datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
             'last_updated': datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)},
            {'currency_id': 2, 'currency_code': 'USD', 'created_at': datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
             'last_updated': datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)},
            {'currency_id': 3, 'currency_code': 'EUR', 'created_at': datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
             'last_updated': datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)}]}


@mark.describe('testing fetch_table_names')
@pytest.mark.skip
def test_fetch_one_table():
    with CreateConnection() as conn:
        table_names = fetch_table_names(conn)
        assert table_names == ['address', 'staff', 'payment', 'department', 'transaction', 'currency', 'payment_type',
                               'sales_order', 'counterparty', 'purchase_order', 'design']


@mark.describe('testing save_all_tables')
@pytest.mark.skip
def test_save_all_tables():
    save_all_tables()
    with open('./db/json_files/db_totes_currency.json', 'r', encoding='utf-8') as f:
        result = json.load(f)
        assert result == {"currency": [
            {"currency_id": 1, "currency_code": "GBP", "created_at": "Thu, 03 Nov 2022 14:20:49 GMT",
             "last_updated": "Thu, 03 Nov 2022 14:20:49 GMT"},
            {"currency_id": 2, "currency_code": "USD", "created_at": "Thu, 03 Nov 2022 14:20:49 GMT",
             "last_updated": "Thu, 03 Nov 2022 14:20:49 GMT"},
            {"currency_id": 3, "currency_code": "EUR", "created_at": "Thu, 03 Nov 2022 14:20:49 GMT",
             "last_updated": "Thu, 03 Nov 2022 14:20:49 GMT"}]}
