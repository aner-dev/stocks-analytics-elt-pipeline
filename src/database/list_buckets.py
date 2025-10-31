from pathlib import Path

from dotenv import load_dotenv
from loguru import logger
import sys
from s3_client import minio_client

logger.remove(0)
logger.add(sys.stderr, format="{time} {level} {message}", level="INFO")

DOTENV_PATH = Path(__file__).resolve().parent.parent.parent / ".airflow.env"
load_dotenv(dotenv_path=DOTENV_PATH)


def list_bucket_files(bucket_name: str):
    try:
        client = minio_client()

        # list objects in bucket
        response = client.list_objects_v2(Bucket=bucket_name)

        if "Contents" not in response:
            logger.warning(f"Bucket '{bucket_name}' is empty")
            return []

        files = []
        for obj in response["Contents"]:
            file_info = {
                "name": obj["Key"],
                "size": obj["Size"],
                "last_modified": obj["LastModified"],
            }
            files.append(file_info)
            logger.info(f" {file_info['name']} ({file_info['size']} bytes)")

        logger.info(f"total files in '{bucket_name}': {len(files)}")
        return files

    except Exception as e:
        logger.error(f"Error at list buckets {e}")
        raise


if __name__ == "__main__":
    list_bucket_files("data")
