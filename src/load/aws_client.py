from datetime import datetime, timezone
import io

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from botocore.exceptions import ClientError
from elt_pipeline_hn.src.load.S3_client import (
    load_to_s3,
    download_from_S3,
    write_gold,
)
from datetime import datetime
import json

import polars as pl

import structlog

log = structlog.get_logger()

AWS_S3_CONN_ID = "aws_s3_localstack_conn"


def get_s3_hook():
    """Initialize S3Hook using connection ID of Airflow."""
    return S3Hook(aws_conn_id=AWS_S3_CONN_ID)


def load_to_s3(data: bytes, bucket: str, object_name: str, content_type: str) -> str:
    """load data to S3 bucket"""
    hook = get_s3_hook()

    if object_name is None:
        raise ValueError("object_name must be provided to load_to_s3")
    # create bucket if it not exists
    if not hook.check_for_bucket(bucket):
        hook.create_bucket(bucket)

    hook.load_bytes(
        bytes_data=data,
        key=object_name,
        bucket_name=bucket,
        replace=True,
    )
    log.info(f"uploaded file {len(data)} bytes to {bucket}/{object_name}")

    return f"{bucket}/{object_name}"


def write_bronze_raw(
    raw_bytes: bytes,
    bucket: str,
    dataset_id: str,
    file_extension: str,
    timestamp: str | None = None,
):
    """load raw bytes (e.g., CSV.GZ) to bronze layer"""
    if timestamp is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # load raw bytes (without decoding or transform to JSON)
    object_name = f"bronze/{dataset_id}/raw_{timestamp}.{file_extension}"

    try:
        result = load_to_s3(
            data=raw_bytes,
            bucket=bucket,
            object_name=object_name,
            content_type="application/octet-stream",  # generic type for raw bytes
        )
        log.info("✔️ load raw bronze layer successfully")
        return result
    except Exception as e:
        log.error(f"write bronze layer operation failed: {e}")
        raise


def write_bronze(
    json_data: dict, bucket: str, symbol: str, timestamp: str | None = None
):
    """load bronze layer to bucket"""

    if timestamp is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    json_bytes = json.dumps(json_data).encode("utf-8")
    object_name = f"bronze/alpha_vantage/{symbol}/weekly_{timestamp}.json"

    try:
        result = load_to_s3(
            data=json_bytes,
            bucket="stocks-data",
            object_name=object_name,
            content_type="application/json",
        )
        log.info(" load bronze layer successfully")

        return result

    except Exception as e:
        log.error(f"write bronze layer operation failed: {e}")
        raise


def write_silver(
    parquet_bytes: bytes,
    bucket: str = "stocks-data",
    symbol: str | None = None,
    timestamp: str | None = None,
):
    """load parquet bytes to silver layer"""

    if timestamp is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if symbol:
        object_name = f"silver/stocks_metrics/{symbol}/weekly_{timestamp}.parquet"
    else:
        date_folder = datetime.now().strftime("%Y%m%d")
        object_name = f"silver/unified_stocks/date={date_folder}/weekly_unified{timestamp}.parquet"

    log.info(f"uploading silver layer: {object_name}")

    return load_to_s3(
        data=parquet_bytes,
        bucket=bucket,
        object_name=object_name,
        content_type="application/x-parquet",
    )


# New function to create in S3_client.py
def write_gold(
    df: pl.DataFrame, bucket: str, table_name: str, partitions: list[str] | None = None
) -> str:
    """Writes a Polars DataFrame to the Gold layer as partitioned Parquet.

    NOTE: Advanced partitioning directly to S3 /S3 requires complex file system
    handling (e.g., using pyarrow's S3FileSystem or similar integration).
    For simplicity with load_to_s3 (which writes single bytes), this implementation
    writes a single non-partitioned Parquet file.
    """

    log.info(f"Starting write for Gold table: {table_name}")

    # 1. Write the DataFrame to an in-memory buffer
    buffer = io.BytesIO()

    # Write Polars DataFrame to the buffer as Parquet
    df.write_parquet(buffer, compression="snappy")
    parquet_bytes = buffer.getvalue()

    # --- Define object key and check partitioning ---

    if partitions:
        # If partitioning is requested, we log a warning since the current simple
        # load_to_s3 cannot handle writing a partitioned directory structure.
        log.warning(
            f"Partitioning requested for {table_name}: {partitions}. "
            "Writing as a single Parquet file due to current load_to_s3 limitations."
        )
        # Use a temporary file name that includes the current timestamp
        object_name = (
            f"gold/{table_name}/{datetime.now().strftime('%Y%m%d%H%M%S')}.parquet"
        )
    else:
        # Default non-partitioned file name
        object_name = f"gold/{table_name}/data_snapshot_{datetime.now().strftime('%Y%m%d')}.parquet"

    log.info(f"Target S3  key: {object_name}")

    # 2. Upload the single Parquet file (bytes) to S3 
    return load_to_s3(
        data=parquet_bytes,
        bucket=bucket,
        object_name=object_name,
        content_type="application/x-parquet",
    )


def download_from_S3(bucket: str, object_name: str) -> bytes:
    """low level driver: download bytes from a minIO object."""

    hook = get_s3_hook()

    try:
        # get_key() returns a S3 key object that containts the metadata and body
        s3_key_object = hook.get_key(key=object_name, bucket_name=bucket)

        if s3_key_object is None:
            raise FileNotFoundError(
                f"Object '{object_name}' not found in bucket '{bucket}'."
            )

        # READ the body object as bytes (file content).
        # this is equivalent to 's3_key_object.get()["Body"].read()' of Boto3.
        with s3_key_object.get()["Body"] as body_stream:
            return body_stream.read()

    except ClientError as e:
        # AWS error handling (e.g., bucket not exist, permission denegated)
        if e.response["Error"]["Code"] == "404":
            raise FileNotFoundError(
                f"Object '{object_name}' not found in bucket '{bucket}'."
            )
        raise ConnectionError(f"S3  connection error: {e}")
    except Exception as e:
        raise ConnectionError(f"Error reading from S3 : {e}")


def read_bronze(bucket: str, symbol: str, object_name: str | None = None):
    hook = get_s3_hook()

    if object_name is None:
        prefix = f"bronze/alpha_vantage/{symbol}/"

        object_list = hook.list_keys(bucket, prefix=prefix, delimiter="/")

        if not object_list:
            log.error(f"no objects found in {bucket} for symbol {symbol}")
            raise ValueError(f"no objects found for symbol {symbol}")

        # hook.list_keys returns a list of strings with the object names
        object_name = sorted(object_list)[-1]

    json_bytes = download_from_S3(bucket, object_name)

    log.info(f"loading {object_name} from {bucket}")
    try:
        data = json.loads(json_bytes.decode("utf-8"))
        log.info("successfully loaded data")
        return data

    except json.JSONDecodeError as e:
        log.error(f"invalid JSON: {e}")
        raise
    except Exception as e:
        log.error(f"error processing bronze layer: {e}")
        raise


def read_parquet_by_key(bucket: str, object_name: str) -> pl.DataFrame:
    """Reads a single Parquet file by its full S3  key and returns a Polars DataFrame."""

    parquet_bytes = download_from_S3(bucket, object_name)

    buffer = io.BytesIO(parquet_bytes)
    df = pl.read_parquet(buffer)

    log.info(f"Successfully read Parquet file: {object_name}")
    return df
