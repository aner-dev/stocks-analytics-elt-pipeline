from s3_client import minio_client
from loguru import logger
from dotenv import load_dotenv
import os

env_path = os.path.join(os.path.dirname(__file__), "..", "..", "config", "minio.env")
load_dotenv(env_path)


def test_minio_connection():
    try:
        client = minio_client()

        response = client.list_buckets()
        buckets = [bucket["Name"] for bucket in response["Buckets"]]

        logger.info("connected to minIO")
        logger.info(f"buckets available: {buckets}")
        return True

    except Exception as e:
        logger.error(f"Error: connection failed {e}")
        return False


if __name__ == "__main__":
    test_minio_connection()
