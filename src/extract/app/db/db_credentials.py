"""Getting from the db_credentials from AWS Secrets Manager"""
import json
import logging
import boto3

def get_db_credentials(secret_name: str):
    """Retrieves the database credentials"""
    try:
        client = boto3.client("secretsmanager", region_name="eu-west-2")
        secret_value = client.get_secret_value(SecretId=secret_name)
        return json.loads(secret_value["SecretString"])
    except Exception as err:
        logging.error('Error retrieving secret {secret_name}: %s', err)
        raise
