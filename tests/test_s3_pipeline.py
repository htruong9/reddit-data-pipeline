"""
Tests for pipelines/aws_s3_pipeline.py.

Uses moto to mock S3 so no real AWS calls are made.
"""

import io
import tempfile
from pathlib import Path
from unittest.mock import patch

import boto3
import pandas as pd
import pytest
from moto import mock_aws

BUCKET = "test-raw-bucket"


@mock_aws
class TestUploadCsvToS3:
    """Upload a local CSV file to a mocked S3 bucket."""

    def _create_bucket(self):
        client = boto3.client("s3", region_name="eu-west-2")
        client.create_bucket(
            Bucket=BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        return client

    @patch("pipelines.aws_s3_pipeline.S3_BUCKET_RAW", BUCKET)
    @patch("pipelines.aws_s3_pipeline.AWS_ACCESS_KEY", "testing")
    @patch("pipelines.aws_s3_pipeline.AWS_SECRET_KEY", "testing")
    def test_upload_csv(self, sample_reddit_df: pd.DataFrame):
        from pipelines.aws_s3_pipeline import upload_csv_to_s3

        client = self._create_bucket()

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            sample_reddit_df.to_csv(f, index=False)
            tmp_path = f.name

        key = upload_csv_to_s3(tmp_path, bucket=BUCKET, key_prefix="raw/2026/03/16/")

        # Verify the object exists
        obj = client.get_object(Bucket=BUCKET, Key=key)
        body = obj["Body"].read().decode("utf-8")
        assert "abc123" in body
        Path(tmp_path).unlink()


@mock_aws
class TestUploadDfToS3:
    """Upload a DataFrame directly to mocked S3."""

    def _create_bucket(self):
        client = boto3.client("s3", region_name="eu-west-2")
        client.create_bucket(
            Bucket=BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        return client

    @patch("pipelines.aws_s3_pipeline.S3_BUCKET_RAW", BUCKET)
    @patch("pipelines.aws_s3_pipeline.AWS_ACCESS_KEY", "testing")
    @patch("pipelines.aws_s3_pipeline.AWS_SECRET_KEY", "testing")
    def test_upload_df(self, sample_reddit_df: pd.DataFrame):
        from pipelines.aws_s3_pipeline import upload_df_to_s3

        client = self._create_bucket()

        key = upload_df_to_s3(
            sample_reddit_df,
            filename="test_upload.csv",
            bucket=BUCKET,
            key_prefix="raw/2026/03/16/",
        )

        assert key == "raw/2026/03/16/test_upload.csv"

        obj = client.get_object(Bucket=BUCKET, Key=key)
        body = obj["Body"].read().decode("utf-8")
        assert "def456" in body
