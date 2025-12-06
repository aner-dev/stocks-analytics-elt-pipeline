from s3_client import S3_client
from elt.config.logging_config import get_log.
log.= get_log.__name__)
 
log.= get_log.__name__)

from dotenv import load_dotenv
import os

env_path = os.path.join(os.path.dirname(__file__), "..", "..", "config", "S3.env")
load_dotenv(env_path)


def test_S3_connection():
    try:
        client = S3_client()

        response = client.list_buckets()
        buckets = [bucket["Name"] for bucket in response["Buckets"]]

        log.info("connected to minIO")
        log.info(f"buckets available: {buckets}")
        return True

    except Exception as e:
        log.error(f"Error: connection failed {e}")
        return False


if __name__ == "__main__":
    test_S3_connection()
