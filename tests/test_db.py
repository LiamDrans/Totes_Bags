"""test script for the db functions"""
from unittest.mock import patch
import pytest
from pytest_postgresql import factories
from pg8000.native import Connection
from pytest import mark
from db.crud_functions import query_db, fetch_one_table, fetch_table_names, fetch_all_tables

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
def test_fetch_one_table_returns_only_one_table(postgresql):
    cur = postgresql.cursor()
    cur.execute("CREATE TABLE test (id serial PRIMARY KEY, num integer, data varchar);")
    cur.execute("INSERT INTO test (num, data) VALUES (1, 'test data');")
    postgresql.commit()
    cur.close()

    database_connection = Connection(
        postgresql.pgconn.user,
        host=postgresql.pgconn.host,
        password=postgresql.pgconn.password,
        database=postgresql.pgconn.db,
        port=postgresql.pgconn.port
    )

    with database_connection as conn:
        result = fetch_one_table('test', conn)
        assert result == {'test': [{'id': 1, 'num': 1, 'data': 'test data'}]}

@mark.describe('testing fetch_table_names')
def test_fetch_table_names(postgresql):
    cur = postgresql.cursor()
    cur.execute("CREATE TABLE test (id serial PRIMARY KEY, num integer, data varchar);")
    cur.execute("INSERT INTO test (num, data) VALUES (1, 'test data');")
    cur.execute("CREATE TABLE test2 (id serial PRIMARY KEY, num integer, data varchar);")
    cur.execute("INSERT INTO test2 (num, data) VALUES (2, 'test data2');")
    postgresql.commit()
    cur.close()

    database_connection = Connection(
        postgresql.pgconn.user,
        host=postgresql.pgconn.host,
        password=postgresql.pgconn.password,
        database=postgresql.pgconn.db,
        port=postgresql.pgconn.port
    )

    with database_connection as conn:
        table_names = fetch_table_names(conn)
        assert table_names == ['test', 'test2']

@mark.describe('testing save_all_tables')
@patch('db.crud_functions.CreateConnection')
def test_fetch_all_tables(MockConnection, postgresql):
    cur = postgresql.cursor()
    cur.execute("CREATE TABLE test (id serial PRIMARY KEY, num integer, data varchar);")
    cur.execute("INSERT INTO test (num, data) VALUES (1, 'test data');")
    cur.execute("CREATE TABLE test2 (id serial PRIMARY KEY, num integer, data varchar);")
    cur.execute("INSERT INTO test2 (num, data) VALUES (2, 'test data2');")
    postgresql.commit()
    cur.close()

    MockConnection.return_value = Connection(
        postgresql.pgconn.user,
        host=postgresql.pgconn.host,
        password=postgresql.pgconn.password,
        database=postgresql.pgconn.db,
        port=postgresql.pgconn.port
    )

    result = fetch_all_tables()
    assert result[0] == {'test': [{'id': 1, 'num': 1, 'data': 'test data'}]}
    assert result[1] == {'test2': [{'id': 1, 'num': 2, 'data': 'test data2'}]}
