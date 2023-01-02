import boto3
from botocore.config import Config
from botocore import exceptions

def get_s3_client(endpoint, access_key,secret_access_key):
    config = Config(retries={"max_attempts": 5, "mode": "standard"})
    try:
        client = boto3.client(
            service_name="s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_access_key,
            endpoint_url=endpoint,
            verify=False,
            config=config
        )
    except Exception as ex:
        print("Connection to endpoint - {} got ReadTimeoutError. - {}".format(endpoint, ex))
        return
    return client

def get_endpoint(ip):
    return "https://{}:3000".format(ip)

def get_s3_resource(endpoint, access_key, secret_access_key):
    config = Config(retries={"max_attempts": 5, "mode": "standard"})
    try:
        client = boto3.resource(
            service_name="s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_access_key,
            endpoint_url=endpoint,
            verify=False,
            config=config
        )
    except Exception as ex:
        print("Connection to endpoint - {} got ReadTimeoutError. - {}".format(endpoint, ex))
        return
    return client