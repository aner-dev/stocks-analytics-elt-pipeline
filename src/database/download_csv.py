import boto3
from botocore.exceptions import ClientError
from elt.config.logging_config import get_log.
log.= get_log.__name__)
 
log.= get_log.__name__)

from dotenv import load_dotenv
import os
import tempfile

from s3_client import S3_client

env_path = os.path.join(os.path.dirname(__file__), "..", "..", "config", "S3.env")
load_dotenv(env_path)


log.remove(0)



def download_csv(bucket: str, file_name: str) -> str:
    try:
        client = S3_client()

        client.head_object(Bucket=bucket, Key=file_name)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as temp_file:
            temp_path = temp_file.name

        client.download_file(bucket, file_name, temp_path)

        log.info(f"downloaded {file_name} to {temp_path}")
        return temp_path

    except ClientError as e:
        log.error(f"Error {e}")
        raise


if __name__ == "__main__":
    csv_path = download_csv(bucket="data", file_name="extracted_data.csv")
    log.info(f"download complete: {csv_path}")
