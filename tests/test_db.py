"""test script for the db functions"""

import json
import datetime
from pytest import mark
from db.connection import CreateConnection
from db.crud_functions import query_db, fetch_one_table, fetch_table_names, save_all_tables

@mark.describe('testing query_db')
class TestQueryDB:

    @mark.it('Test connection to database and pulls one row')
    def test_query_db(self):
        sql = "SELECT * from currency LIMIT 1;"
        result = query_db(sql)
        assert result == [[1, 'GBP', datetime.datetime(2022, 11, 3, 14, 20, 49, 962000), datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)]]

@mark.describe('testing fetch_one_table')
class TestFetchOneTable:

    def test_fetch_one_table(self):
        with CreateConnection() as conn:
            result = fetch_one_table('currency', conn)
            assert result == {'currency': [{'currency_id': 1, 'currency_code': 'GBP', 'created_at': datetime.datetime(2022, 11, 3, 14, 20, 49, 962000), 'last_updated': datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)}, {'currency_id': 2, 'currency_code': 'USD', 'created_at': datetime.datetime(2022, 11, 3, 14, 20, 49, 962000), 'last_updated': datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)}, {'currency_id': 3, 'currency_code': 'EUR', 'created_at': datetime.datetime(2022, 11, 3, 14, 20, 49, 962000), 'last_updated': datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)}]}

@mark.describe('testing fetch_table_names')
class TestFetchTableNames:

    def test_fetch_one_table(self):
        with CreateConnection() as conn:
            table_names = fetch_table_names(conn)
            assert table_names == ['address', 'staff', 'payment', 'department', 'transaction', 'currency', 'payment_type', 'sales_order', 'counterparty', 'purchase_order', 'design']

@mark.describe('testing save_all_tables')
class TestSaveAllTables:

    def test_save_all_tables(self):
        save_all_tables()
        with open('./db/json_files/db_totes_currency.json', 'r', encoding='utf-8') as f:
            result = json.load(f)
            assert result == {"currency":[{"currency_id":1,"currency_code":"GBP","created_at":"Thu, 03 Nov 2022 14:20:49 GMT","last_updated":"Thu, 03 Nov 2022 14:20:49 GMT"},{"currency_id":2,"currency_code":"USD","created_at":"Thu, 03 Nov 2022 14:20:49 GMT","last_updated":"Thu, 03 Nov 2022 14:20:49 GMT"},{"currency_id":3,"currency_code":"EUR","created_at":"Thu, 03 Nov 2022 14:20:49 GMT","last_updated":"Thu, 03 Nov 2022 14:20:49 GMT"}]}
            