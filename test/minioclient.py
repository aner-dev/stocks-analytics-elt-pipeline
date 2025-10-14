from minio import Minio
from minio.error import S3Error


def load_data():
    client = Minio(
        endpoint="localhost:9000",
        access_key="eltadmin",
        secret_key="eltpassword",
        secure=False,
    )

    source_file = "miniotest.txt"

    bucket_name = "minio1"
    destination_file = "miniotest.txt"
    # Make the bucket if it doesn't exist.
    found = client.bucket_exists(bucket_name=bucket_name)
    if not found:
        client.make_bucket(bucket_name=bucket_name)
        print("Created bucket", bucket_name)
    else:
        print("Bucket", bucket_name, "already exists")

    # Upload the file, renaming it in the process
    client.fput_object(
        bucket_name=bucket_name,
        object_name=destination_file,
        file_path=source_file,
    )
    print(
        source_file,
        "successfully uploaded as object",
        destination_file,
        "to bucket",
        bucket_name,
    )


if __name__ == "__main__":
    try:
        load_data()
    except S3Error as exc:
        print("error occurred.", exc)
