"""Getting from the db_credentials from AWS Secrets Manager"""

import json
import boto3


def get_db_credentials(secret_name):
    """Retrieves the database credentials"""

    client = boto3.client("secretsmanager", region_name="eu-west-2")
    secret_value = client.get_secret_value(SecretId=secret_name)
    return json.loads(secret_value["SecretString"])
