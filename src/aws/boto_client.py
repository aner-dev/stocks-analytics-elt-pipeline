from datetime import datetime
import os
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import structlog
import json
from typing import BinaryIO

log = structlog.get_logger()

RUSTFS_ENDPOINT = os.environ.get("AWS_ENDPOINT_URL", "http://rustfs:9000")

S3_REGION = "us-east-1"
# -------------------------------


def ensure_bucket_exists(bucket: str) -> None:
    s3 = get_s3_client()

    try:
        s3.head_bucket(Bucket=bucket)
        log.info("S3 bucket already exists", bucket=bucket)
        return
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code in ("404", "NoSuchBucket"):
            log.info("Creating S3 bucket", bucket=bucket)
            s3.create_bucket(Bucket=bucket)
        else:
            raise


def _get_boto3_client_generic(
    service: str, endpoint: str | None = None, region: str = S3_REGION
):
    """
     Generic helper to create a boto3 client for any S3-compatible backend
    (RustFS, MinIO, LocalStack, AWS).
    """
    config_local = Config(
        signature_version="s3v4",
        connect_timeout=5,
        read_timeout=3600,
    )

    boto3_session = boto3.session.Session(
        # Use hardcoded or environment credentials; 'test' is the standard for LocalStack
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "rustfsadmin"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "rustfsadmin"),
        region_name=region,
    )

    client = boto3_session.client(
        service,
        endpoint_url=endpoint,
        config=config_local,
    )
    return client


def get_s3_client():
    """Initializes an S3 client for an S3-compatible backend (RustFS/MinIO/AWS)."""
    return _get_boto3_client_generic(service="s3", endpoint=RUSTFS_ENDPOINT)


def load_to_bronze(data: bytes, bucket: str, object_name: str, content_type: str):
    """
    Loads data to S3 (LocalStack) using the direct Boto3 client.
    Replaces the 'load_to_bronze' function that used the Airflow hook.
    """
    s3_client = get_s3_client()
    try:
        s3_client.head_bucket(Bucket=bucket)
    except ClientError as e:
        raise RuntimeError(f"S3 bucket does not exist or is invalid: {bucket}") from e

    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=object_name,
            Body=data,
            ContentType=content_type,
        )
        return {"bucket": bucket, "key": object_name}
    except ClientError as e:
        log.error("S3 PutObject failed", bucket=bucket, key=object_name, error=str(e))
        raise


def download_from_s3(bucket: str, object_name: str) -> bytes:
    """
    Downloads the binary content of an object from S3 (LocalStack).
    Replaces the 'download_from_S3' function that used the Airflow hook.
    """
    s3_client = get_s3_client()
    try:
        response = s3_client.get_object(Bucket=bucket, Key=object_name)
        # Read the entire body as bytes
        return response["Body"].read()
    except ClientError as e:
        log.error(f"S3 GetObject failed for {object_name}: {e}")
        raise
