import boto3
from botocore.exceptions import ClientError
from loguru import logger
from dotenv import load_dotenv
import os
import tempfile
import sys
from s3_client import minio_client

env_path = os.path.join(os.path.dirname(__file__), "..", "..", "config", "minio.env")
load_dotenv(env_path)


logger.remove(0)
logger.add(sys.stderr, format="{time} {level} {message}", level="INFO", serialize=True)


def download_csv(bucket: str, file_name: str) -> str:
    try:
        client = minio_client()

        client.head_object(Bucket=bucket, Key=file_name)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as temp_file:
            temp_path = temp_file.name

        client.download_file(bucket, file_name, temp_path)

        logger.info(f"downloaded {file_name} to {temp_path}")
        return temp_path

    except ClientError as e:
        logger.error(f"Error {e}")
        raise


if __name__ == "__main__":
    csv_path = download_csv(bucket="data", file_name="extracted_data.csv")
    logger.info(f"download complete: {csv_path}")
