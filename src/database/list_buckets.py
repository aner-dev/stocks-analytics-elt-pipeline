

from dotenv import load_dotenv
from elt.config.logging_config import get_log.
log.= get_log.__name__)
 
log.= get_log.__name__)


from s3_client import S3_client

log.remove(0)


DOTENV_PATH = Path(__file__).resolve().parent.parent.parent / ".airflow.env"
load_dotenv(dotenv_path=DOTENV_PATH)


def list_bucket_files(bucket_name: str):
    try:
        client = S3_client()

        # list objects in bucket
        response = client.list_objects_v2(Bucket=bucket_name)

        if "Contents" not in response:
            log.warning(f"Bucket '{bucket_name}' is empty")
            return []

        files = []
        for obj in response["Contents"]:
            file_info = {
                "name": obj["Key"],
                "size": obj["Size"],
                "last_modified": obj["LastModified"],
            }
            files.append(file_info)
            log.info(f" {file_info['name']} ({file_info['size']} bytes)")

        log.info(f"total files in '{bucket_name}': {len(files)}")
        return files

    except Exception as e:
        log.error(f"Error at list buckets {e}")
        raise


if __name__ == "__main__":
    list_bucket_files("data")
