import json
from datetime import datetime

import structlog

from src.aws.boto_client import download_from_s3, get_s3_client, load_to_bronze
from src.config.settings import S3_BUCKET

log = structlog.get_logger()


def write_bronze(json_data: dict, symbol: str, timestamp: str | None = None) -> dict:
    """load bronze layer to bucket"""

    now = datetime.now()
    date_folder = now.strftime("%Y-%m-%d")
    if timestamp is None:
        timestamp = datetime.now().strftime("%H%M%S")

    object_name = (
        f"bronze/alpha_vantage/{symbol}/weekly_{timestamp}.json"
        f"date={date_folder}/weekly_{timestamp}.json"
    )

    try:
        json_bytes = json.dumps(json_data).encode("utf-8")

        load_to_bronze(
            data=json_bytes,
            bucket=S3_BUCKET,  # Use the bucket from the function for flexibility
            object_name=object_name,
            content_type="application/json",
        )
        log.info(
            "  Bronze load successful",
            bucket=S3_BUCKET,
            key=object_name,
            symbol=symbol,
        )

        # Return the data that must be read immediately after
        # Although only the response dictionary is used here
        # it's good practice to return the data if it's small
        # For the purpose of Data Discovery, we need to return the loaded data:
        return {"symbol": symbol, "bucket": S3_BUCKET, "key": object_name}

    except Exception as e:
        log.error("Bronze load failed", symbol=symbol, error=str(e))
        raise


def read_bronze_by_symbol(
    bucket: str, symbol: str, object_name: str | None = None
) -> dict:  # Added return type hint
    """Reads the latest JSON object from the bronze layer for the given symbol."""

    s3_client = get_s3_client()
    prefix = f"bronze/alpha_vantage/{symbol}/"

    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

    if "Contents" not in response:
        log.error(f"no objects found in {bucket} for prefix {prefix}")
        raise ValueError(f"no objects found for symbol {symbol}")

    # Sort by LastModified (upload date) to get the latest
    objects = sorted(response["Contents"], key=lambda x: x["LastModified"])

    if not objects:
        log.error(f"no objects found in {bucket} for symbol {symbol}")
        raise ValueError(f"no objects found for symbol {symbol}")

    object_name = objects[-1]["Key"]

    # **Adjustment:** Use the direct Boto3 download_from_s3 function
    json_bytes = download_from_s3(bucket, object_name)

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


def read_bronze_by_key(object_name: str) -> dict:
    """Reads a specific JSON object from the bronze layer by its full S3 key."""

    json_bytes = download_from_s3(S3_BUCKET, object_name)

    try:
        return json.loads(json_bytes.decode("utf-8"))
    except json.JSONDecodeError as e:
        log.error(f"invalid JSON in bronze: {e}", bucket=S3_BUCKET, key=object_name)
        raise
