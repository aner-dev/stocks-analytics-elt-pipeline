import boto3
import os


def minio_client() -> boto3.client:
    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    minio_user = os.getenv("MINIO_ROOT_USER")
    minio_password = os.getenv("MINIO_ROOT_PASSWORD")

    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=minio_user,
        aws_secret_access_key=minio_password,
        config=boto3.session.Config(signature_version="s3v4"),
    )
    return client
