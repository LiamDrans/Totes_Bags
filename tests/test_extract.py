from unittest.mock import patch
import json
import boto3
from moto import mock_aws
from pytest import mark
from pg8000.native import Connection
from src.extract.extract import lambda_handler
from src.extract.utils.json import json_encode

@mark.it('Test lambda function successfully upload full database on first run')
@mock_aws
@patch('src.extract.db.db_crud_functions.CreateConnection')
def test_lambda_handler_upload(MockConnection, postgresql):
    cur = postgresql.cursor()
    cur.execute("CREATE TABLE test (id serial PRIMARY KEY, num integer, last_updated timestamp);")
    cur.execute("INSERT INTO test (num, last_updated) VALUES (1, '2022-11-03 14:20:49.962');")
    cur.execute("CREATE TABLE test2 (id serial PRIMARY KEY, num integer, last_updated timestamp);")
    cur.execute("INSERT INTO test2 (num, last_updated) VALUES (2, '2022-11-03 14:20:49.962');")
    postgresql.commit()
    cur.close()

    MockConnection.return_value = Connection(
        postgresql.pgconn.user,
        host=postgresql.pgconn.host,
        password=postgresql.pgconn.password,
        database=postgresql.pgconn.db,
        port=postgresql.pgconn.port
    )

    s3 = boto3.client('s3', region_name='us-east-1')
    s3.create_bucket(Bucket='totes-data-1234124')
    lambda_handler(event=True,context=True)
    response = s3.list_objects(Bucket='totes-data-1234124')
    result = s3.get_object(Bucket='totes-data-1234124', Key='full_db_totes.json')
    assert response['Contents'][1]['Key'] == 'full_db_totes.json'
    assert json.load(result["Body"]) == [
        {'test': [{'id': 1, 'num': 1, 'last_updated': 'Thu, 03 Nov 2022 14:20:49 GMT'}]},
        {'test2': [{'id': 1, 'num': 2, 'last_updated': 'Thu, 03 Nov 2022 14:20:49 GMT'}]}
    ]


@mark.it('Test lambda function successfully uploads only the updates when run')
@mock_aws
@patch('src.extract.db.db_crud_functions.CreateConnection')
def test_lambda_handler_update(MockConnection, postgresql):
    cur = postgresql.cursor()
    cur.execute("CREATE TABLE test (id serial PRIMARY KEY, num integer, last_updated timestamp);")
    cur.execute("INSERT INTO test (num, last_updated) VALUES (1, '2022-11-03 14:20:49.962');")
    cur.execute("CREATE TABLE test2 (id serial PRIMARY KEY, num integer, last_updated timestamp);")
    cur.execute("INSERT INTO test2 (num, last_updated) VALUES (2, '2022-11-03 14:20:49.962');")
    postgresql.commit()

    MockConnection.return_value = Connection(
        postgresql.pgconn.user,
        host=postgresql.pgconn.host,
        password=postgresql.pgconn.password,
        database=postgresql.pgconn.db,
        port=postgresql.pgconn.port
    )

    test_data = [
        {'test': [{'id': 1, 'num': 1, 'last_updated': 'Thu, 03 Nov 2022 14:20:49 GMT'}]},
        {'test2': [{'id': 1, 'num': 2, 'last_updated': 'Thu, 03 Nov 2022 14:20:49 GMT'}]}
    ]

    s3 = boto3.client('s3', region_name='us-east-1')
    s3.create_bucket(Bucket='totes-data-1234124')
    s3.put_object(
        Body= json_encode(test_data),
        Bucket='totes-data-1234124',
        Key='full_db_totes.json'
    )

    cur = postgresql.cursor()
    cur.execute("INSERT INTO test (num, last_updated) VALUES (2, Now());")
    postgresql.commit()
    cur.close()

    lambda_handler(event=True,context=True)
    response = s3.list_objects(Bucket='totes-data-1234124')

    assert response['Contents'][2]['Key'] == 'latest_db_totes.json'
    result = s3.get_object(Bucket='totes-data-1234124', Key='latest_db_totes.json')
    body = json.load(result['Body'])

    assert body[0]['test'][0]['id'] == 2
    assert body[0]['test'][0]['num'] == 2


@mark.it('Test lambda handler with no updates')
@mock_aws
@patch('src.extract.db.db_crud_functions.CreateConnection')
def test_lambda_handler_no_update(MockConnection, postgresql):
    cur = postgresql.cursor()
    cur.execute("CREATE TABLE test (id serial PRIMARY KEY, num integer, last_updated timestamp);")
    cur.execute("INSERT INTO test (num, last_updated) VALUES (1, '2022-11-03 14:20:49.962');")
    cur.execute("CREATE TABLE test2 (id serial PRIMARY KEY, num integer, last_updated timestamp);")
    cur.execute("INSERT INTO test2 (num, last_updated) VALUES (2, '2022-11-03 14:20:49.962');")
    postgresql.commit()

    MockConnection.return_value = Connection(
        postgresql.pgconn.user,
        host=postgresql.pgconn.host,
        password=postgresql.pgconn.password,
        database=postgresql.pgconn.db,
        port=postgresql.pgconn.port
    )

    test_data = [
        {'test': [{'id': 1, 'num': 1, 'last_updated': 'Thu, 03 Nov 2022 14:20:49 GMT'}]},
        {'test2': [{'id': 1, 'num': 2, 'last_updated': 'Thu, 03 Nov 2022 14:20:49 GMT'}]}
    ]

    s3 = boto3.client('s3', region_name='us-east-1')
    s3.create_bucket(Bucket='totes-data-1234124')
    s3.put_object(
        Body= json_encode(test_data),
        Bucket='totes-data-1234124',
        Key='full_db_totes.json'
    )

    assert not lambda_handler(event=True,context=True)
