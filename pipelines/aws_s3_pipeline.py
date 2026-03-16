"""
Pipeline for uploading data to Amazon S3.

Handles both CSV uploads (from the local staging area) and direct
DataFrame-to-S3 writes.
"""

from __future__ import annotations

import functools
import logging
from io import StringIO
from pathlib import Path

import boto3
import pandas as pd

from utils.constants import AWS_ACCESS_KEY, AWS_REGION, AWS_SECRET_KEY, S3_BUCKET_RAW
from utils.helpers import s3_date_prefix, utc_now_str

logger = logging.getLogger(__name__)


import functools


@functools.lru_cache(maxsize=1)
def _get_s3_client():
    """Return a cached boto3 S3 client.

    Credentials are taken from the config file when present.  If both keys
    are ``None`` (e.g. running on EC2 with an instance profile) boto3 picks
    up credentials automatically from the standard credential chain.
    """
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY or None,
        aws_secret_access_key=AWS_SECRET_KEY or None,
    )


def upload_csv_to_s3(
    filepath: str | Path,
    bucket: str | None = None,
    key_prefix: str | None = None,
) -> str:
    """
    Upload a local CSV file to the raw S3 bucket.

    Parameters
    ----------
    filepath : str or Path
        Local path to the CSV file.
    bucket : str, optional
        Target S3 bucket (defaults to ``S3_BUCKET_RAW``).
    key_prefix : str, optional
        S3 key prefix (defaults to date-partitioned path).

    Returns
    -------
    str
        Full S3 key of the uploaded object.
    """
    bucket = bucket or S3_BUCKET_RAW
    key_prefix = key_prefix or s3_date_prefix()
    filepath = Path(filepath)

    s3_key = f"{key_prefix}{filepath.name}"

    logger.info("Uploading %s → s3://%s/%s", filepath, bucket, s3_key)
    client = _get_s3_client()
    client.upload_file(str(filepath), bucket, s3_key)
    logger.info("Upload complete")

    return s3_key


def upload_df_to_s3(
    df: pd.DataFrame,
    filename: str | None = None,
    bucket: str | None = None,
    key_prefix: str | None = None,
) -> str:
    """
    Write a DataFrame directly to S3 as CSV without touching the local filesystem.

    Returns
    -------
    str
        Full S3 key of the uploaded object.
    """
    bucket = bucket or S3_BUCKET_RAW
    key_prefix = key_prefix or s3_date_prefix()
    filename = filename or f"reddit_posts_{utc_now_str()}.csv"

    s3_key = f"{key_prefix}{filename}"
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    logger.info("Writing DataFrame (%d rows) → s3://%s/%s", len(df), bucket, s3_key)
    client = _get_s3_client()
    client.put_object(Bucket=bucket, Key=s3_key, Body=csv_buffer.getvalue())
    logger.info("Write complete")

    return s3_key
