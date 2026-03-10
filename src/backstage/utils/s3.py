"""
S3-compatible object storage client for backstage.

Thin wrapper around boto3 configured via environment variables.
Works with both Hetzner Object Storage and local MinIO.
"""

import json
import os
import time
from typing import Optional

import boto3
from botocore.exceptions import ConnectionClosedError, ClientError


def get_client():
    """Create a boto3 S3 client from environment variables."""
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT_URL"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
    )


def get_bucket() -> str:
    """Get the configured bucket name."""
    return os.environ.get("S3_BUCKET", "openstage-data")


def upload(local_path: str, s3_key: str, bucket: Optional[str] = None) -> None:
    """Upload a local file to S3."""
    client = get_client()
    client.upload_file(local_path, bucket or get_bucket(), s3_key)


def upload_bytes(data: bytes, s3_key: str, bucket: Optional[str] = None) -> None:
    """Upload raw bytes to S3."""
    client = get_client()
    client.put_object(Bucket=bucket or get_bucket(), Key=s3_key, Body=data)


def download(s3_key: str, local_path: str, bucket: Optional[str] = None) -> None:
    """Download a file from S3 to local path."""
    client = get_client()
    client.download_file(bucket or get_bucket(), s3_key, local_path)


def read_bytes(s3_key: str, bucket: Optional[str] = None, retries: int = 3) -> bytes:
    """Read raw bytes from S3 with retry on transient connection errors."""
    for attempt in range(retries):
        try:
            client = get_client()
            response = client.get_object(Bucket=bucket or get_bucket(), Key=s3_key)
            return response["Body"].read()
        except (ConnectionClosedError, ClientError) as e:
            if attempt == retries - 1:
                raise
            time.sleep(2 ** attempt)


def read_json(s3_key: str, bucket: Optional[str] = None) -> dict:
    """Read and parse a JSON file from S3."""
    data = read_bytes(s3_key, bucket)
    return json.loads(data)


def write_json(obj: dict, s3_key: str, bucket: Optional[str] = None) -> None:
    """Write a dict as JSON to S3."""
    data = json.dumps(obj, indent=2, ensure_ascii=False).encode("utf-8")
    upload_bytes(data, s3_key, bucket)


def list_objects(prefix: str, bucket: Optional[str] = None) -> list[dict]:
    """List objects under a prefix. Returns list of dicts with Key and Size."""
    client = get_client()
    bucket_name = bucket or get_bucket()
    result = []

    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get("Contents", []):
            result.append({"Key": obj["Key"], "Size": obj["Size"]})

    return result


def exists(s3_key: str, bucket: Optional[str] = None) -> bool:
    """Check if an object exists in S3."""
    client = get_client()
    try:
        client.head_object(Bucket=bucket or get_bucket(), Key=s3_key)
        return True
    except client.exceptions.ClientError:
        return False
