import os
import io
import boto3
import pandas as pd

from ..utils.logger import setup_logger

logger = setup_logger(__name__)


def read_csv_from_minio():
    endpoint_url = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
    bucket = os.getenv("MINIO_BUCKET", "folder-source")
    csv_key = os.getenv("MINIO_CSV_KEY", "fashion_store_sales.csv")

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

    logger.info(f"Reading s3://{bucket}/{csv_key}")
    response = s3.get_object(Bucket=bucket, Key=csv_key)
    csv_bytes = response["Body"].read()

    return pd.read_csv(io.BytesIO(csv_bytes))
