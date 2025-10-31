from datetime import datetime
import io
import json
import os
from pathlib import Path
import sys

from dotenv import load_dotenv
from loguru import logger
from minio import Minio
from minio.error import S3Error


DOTENV_PATH = Path(__file__).resolve().parent.parent.parent / ".airflow.env"
load_dotenv(dotenv_path=DOTENV_PATH)


def get_minio_client() -> Minio:
    required_vars = {
        "MINIO_ENDPOINT": os.getenv("MINIO_ENDPOINT"),
        "MINIO_ROOT_USER": os.getenv("MINIO_ROOT_USER"),
        "MINIO_ROOT_PASSWORD": os.getenv("MINIO_ROOT_PASSWORD"),
    }

    missing = [var for var, value in required_vars.items() if not value]
    if missing:
        raise ValueError(f"Missing environment variables: {missing}")

    client = Minio(
        endpoint=required_vars["MINIO_ENDPOINT"],
        access_key=required_vars["MINIO_ROOT_USER"],
        secret_key=required_vars["MINIO_ROOT_PASSWORD"],
        secure=False,
    )
    return client


def load_to_minio(data: bytes, bucket: str, object_name: str = None):
    client = get_minio_client()

    if object_name is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        object_name = f"alpha_vantage/IBM/weekly_{timestamp}.json"

    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    data_stream = io.BytesIO(data)
    client.put_object(
        bucket_name=bucket,
        object_name=object_name,
        data=data_stream,
        length=len(data),
        content_type="application/json",
    )
    logger.info(f"uploaded file {len(data)} bytes to {bucket}/{object_name}")

    return f"{bucket}/{object_name}"


def load_raw_stock(bucket: str, object_name: str = None):
    client = get_minio_client()

    if not client.bucket_exists(bucket):
        logger.error(f"{bucket} not found")
        raise ValueError("bucket not found")

    if object_name is None:
        objects = list(client.list_objects(bucket, prefix="alpha_vantage/IBM/"))
        object_list = [obj.object_name for obj in objects]
        if not object_list:
            logger.error(f"no objects found in {bucket}")
            raise ValueError("not objects found in bucket")
        object_name = sorted(object_list)[-1]

    logger.info(f"loading {object_name} from {bucket}")
    try:
        with client.get_object(bucket, object_name) as response:
            json_bytes = response.read()
            data = json.loads(json_bytes.decode("utf-8"))

        logger.info("successfully loaded data")
        return data

    except S3Error as e:
        logger.error(f"Minio error: {e}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"invalid JSON: {e}")
        raise


if __name__ == "__main__":
    logger.remove(0)
    logger.add(sys.stderr, format="{time} {level} {message}", level="INFO")
    try:
        test_data = json.dumps({"test": "data"}).encode("utf-8")
        result = load_to_minio(test_data, "test-bucket")
        logger.info(f"test successful: {result}")
    except Exception as exc:
        logger.error(f"test failed. {exc}")
