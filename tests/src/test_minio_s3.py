import boto3
import pytest
import os
import uuid
from botocore.client import Config
from io import BytesIO


S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://10.0.0.27:30091")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "admin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "password123")
S3_BUCKET = "audio"


@pytest.fixture(scope="module")
def s3_client():
    """
    Initialize the boto3 client to connect to MinIO
    """
    client = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",  # MinIO ignores region by default, but boto3 requires a value
    )
    return client


@pytest.fixture(scope="function")
def test_bucket(s3_client):
    """
    Fixture:
    1. Create a temporary bucket with a random name
    2. Pass the bucket name to the test function
    3. After the test, automatically empty and delete the bucket (Teardown)
    """
    bucket_name = f"test-bucket-{uuid.uuid4().hex}"

    # 1. Create bucket
    try:
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"\n[Setup] Created bucket: {bucket_name}")
    except Exception as e:
        pytest.fail(f"Cannot create bucket: {e}")

    yield bucket_name

    # 3. Cleanup (Teardown)
    print(f"\n[Teardown] Cleaning up bucket: {bucket_name}")
    try:
        # Must delete all objects in the bucket before deleting the bucket itself
        objects = s3_client.list_objects_v2(Bucket=bucket_name)
        if "Contents" in objects:
            for obj in objects["Contents"]:
                s3_client.delete_object(Bucket=bucket_name, Key=obj["Key"])

        # Delete empty bucket
        s3_client.delete_bucket(Bucket=bucket_name)
    except Exception as e:
        print(f"Failed to clean up bucket: {e}")


def test_upload_and_download(s3_client, test_bucket):
    """
    Core test logic:
    1. Upload a text string
    2. Download the text
    3. Compare contents for equality
    """
    file_name = "test_data.txt"
    content_to_upload = b"Hello MinIO! This is a test string."

    # --- 1. Upload ---
    print(f"[*] Uploading {file_name}...")
    try:
        s3_client.put_object(
            Bucket=test_bucket,
            Key=file_name,
            Body=content_to_upload,
            ContentType="text/plain",
        )
    except Exception as e:
        pytest.fail(f"Upload failed: {e}")

    # --- 2. Verify metadata (Optional) ---
    # Confirm the file actually exists
    try:
        s3_client.head_object(Bucket=test_bucket, Key=file_name)
    except Exception:
        pytest.fail("File not found after upload (HeadObject failed)")

    # --- 3. Download ---
    print(f"[*] Downloading {file_name}...")
    try:
        response = s3_client.get_object(Bucket=test_bucket, Key=file_name)
        downloaded_content = response["Body"].read()
    except Exception as e:
        pytest.fail(f"Download failed: {e}")

    # --- 4. Assert ---
    assert (
        downloaded_content == content_to_upload
    ), f"Content mismatch! Expected: {content_to_upload}, Actual: {downloaded_content}"

    print("[Pass] Upload and Download check successful.")


def test_list_objects(s3_client, test_bucket):
    """
    Additional test: verify the list objects functionality
    """
    s3_client.put_object(Bucket=test_bucket, Key="file1.txt", Body=b"1")
    s3_client.put_object(Bucket=test_bucket, Key="file2.txt", Body=b"2")

    response = s3_client.list_objects_v2(Bucket=test_bucket)

    # Assert there should be 2 files
    assert response["KeyCount"] == 2
    keys = [obj["Key"] for obj in response["Contents"]]
    assert "file1.txt" in keys
    assert "file2.txt" in keys
