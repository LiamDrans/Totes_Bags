"""Getting from the db_credentials from AWS Secrets Manager"""
import json
import boto3

def get_db_credentials(secret_name: str):
    """Retrieves the database credentials"""
    try:
        client = boto3.client("secretsmanager", region_name="eu-west-2")
        secret_value = client.get_secret_value(SecretId=secret_name)
        return json.loads(secret_value["SecretString"])
    except Exception as e:
        print(f"Error in getting database credentials: {e}")
        return str(e)
