"""
Tests for pipelines/aws_glue_pipeline.py.

Uses moto to mock Glue so no real AWS calls are made.
"""

from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import boto3
import pytest
from moto import mock_aws

DATABASE = "reddit_db"
CRAWLER = "reddit_crawler"
JOB = "reddit_transform_job"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _create_glue_database(client):
    client.create_database(DatabaseInput={"Name": DATABASE})


def _create_glue_crawler(client):
    client.create_crawler(
        Name=CRAWLER,
        Role="arn:aws:iam::123456789012:role/GlueRole",
        DatabaseName=DATABASE,
        Targets={"S3Targets": [{"Path": "s3://bucket/raw/"}]},
    )


def _create_glue_job(client):
    client.create_job(
        Name=JOB,
        Role="arn:aws:iam::123456789012:role/GlueRole",
        Command={"Name": "glueetl", "ScriptLocation": "s3://bucket/script.py"},
    )


# ---------------------------------------------------------------------------
# should_run_crawler
# ---------------------------------------------------------------------------

@mock_aws
class TestShouldRunCrawler:
    @patch("pipelines.aws_glue_pipeline.GLUE_DATABASE", DATABASE)
    def test_returns_true_when_no_tables(self):
        """Crawler should run when the database has no tables yet."""
        from pipelines.aws_glue_pipeline import should_run_crawler

        client = boto3.client("glue", region_name="eu-west-2")
        _create_glue_database(client)

        assert should_run_crawler(DATABASE) is True

    @patch("pipelines.aws_glue_pipeline.GLUE_DATABASE", DATABASE)
    def test_returns_false_when_tables_exist(self):
        """Crawler should be skipped when tables are already catalogued."""
        from pipelines.aws_glue_pipeline import should_run_crawler

        client = boto3.client("glue", region_name="eu-west-2")
        _create_glue_database(client)
        client.create_table(
            DatabaseName=DATABASE,
            TableInput={"Name": "reddit_posts"},
        )

        assert should_run_crawler(DATABASE) is False

    @patch("pipelines.aws_glue_pipeline.GLUE_DATABASE", DATABASE)
    def test_returns_true_when_database_missing(self):
        """Crawler should run when the database does not exist yet."""
        from pipelines.aws_glue_pipeline import should_run_crawler

        # No database created — EntityNotFoundException expected internally
        assert should_run_crawler("nonexistent_db") is True


# ---------------------------------------------------------------------------
# start_crawler
# ---------------------------------------------------------------------------

@mock_aws
class TestStartCrawler:
    @patch("pipelines.aws_glue_pipeline.GLUE_CRAWLER", CRAWLER)
    @patch("pipelines.aws_glue_pipeline.GLUE_DATABASE", DATABASE)
    @patch("pipelines.aws_glue_pipeline.AWS_ACCESS_KEY", None)
    @patch("pipelines.aws_glue_pipeline.AWS_SECRET_KEY", None)
    def test_skipped_when_tables_exist(self):
        """start_crawler returns 'SKIPPED' when tables are already catalogued."""
        from pipelines.aws_glue_pipeline import start_crawler

        client = boto3.client("glue", region_name="eu-west-2")
        _create_glue_database(client)
        client.create_table(
            DatabaseName=DATABASE,
            TableInput={"Name": "reddit_posts"},
        )

        result = start_crawler(crawler_name=CRAWLER, wait=False)
        assert result == "SKIPPED"

    @patch("pipelines.aws_glue_pipeline.GLUE_CRAWLER", CRAWLER)
    @patch("pipelines.aws_glue_pipeline.GLUE_DATABASE", DATABASE)
    @patch("pipelines.aws_glue_pipeline.AWS_ACCESS_KEY", None)
    @patch("pipelines.aws_glue_pipeline.AWS_SECRET_KEY", None)
    def test_force_bypasses_skip(self):
        """force=True should start the crawler even when tables exist."""
        from pipelines.aws_glue_pipeline import start_crawler

        client = boto3.client("glue", region_name="eu-west-2")
        _create_glue_database(client)
        client.create_table(
            DatabaseName=DATABASE,
            TableInput={"Name": "reddit_posts"},
        )
        _create_glue_crawler(client)

        # wait=False so we don't block on polling; just check it started
        result = start_crawler(crawler_name=CRAWLER, wait=False, force=True)
        assert result == "STARTED"


# ---------------------------------------------------------------------------
# start_glue_job
# ---------------------------------------------------------------------------

@mock_aws
class TestStartGlueJob:
    @patch("pipelines.aws_glue_pipeline.GLUE_JOB", JOB)
    @patch("pipelines.aws_glue_pipeline.AWS_ACCESS_KEY", None)
    @patch("pipelines.aws_glue_pipeline.AWS_SECRET_KEY", None)
    def test_starts_job_and_returns_run_id(self):
        """start_glue_job should trigger a run and return without blocking."""
        from pipelines.aws_glue_pipeline import start_glue_job

        client = boto3.client("glue", region_name="eu-west-2")
        _create_glue_job(client)

        result = start_glue_job(job_name=JOB, wait=False)
        assert result == "STARTED"
