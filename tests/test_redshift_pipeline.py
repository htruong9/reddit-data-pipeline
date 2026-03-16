"""
Tests for pipelines/aws_redshift_pipeline.py.

Uses moto to mock Redshift and psycopg2 to avoid real DB connections.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import boto3
import pytest
from moto import mock_aws

CLUSTER_ID = "reddit-cluster"
DATABASE = "reddit_analytics"
BUCKET = "test-transformed-bucket"
IAM_ROLE = "arn:aws:iam::123456789012:role/RedshiftS3ReadRole"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _create_redshift_cluster(client):
    """Provision a minimal moto Redshift cluster."""
    client.create_cluster(
        ClusterIdentifier=CLUSTER_ID,
        NodeType="dc2.large",
        MasterUsername="admin",
        MasterUserPassword="Password1!",
        DBName=DATABASE,
        ClusterType="single-node",
    )
    # moto returns the cluster immediately; wait for it to show an endpoint
    waiter = client.get_waiter("cluster_available")
    waiter.wait(ClusterIdentifier=CLUSTER_ID)


# ---------------------------------------------------------------------------
# create_table
# ---------------------------------------------------------------------------

@mock_aws
class TestCreateTable:
    @patch("pipelines.aws_redshift_pipeline.REDSHIFT_CLUSTER", CLUSTER_ID)
    @patch("pipelines.aws_redshift_pipeline.REDSHIFT_DATABASE", DATABASE)
    @patch("pipelines.aws_redshift_pipeline.REDSHIFT_USER", "admin")
    @patch("pipelines.aws_redshift_pipeline.REDSHIFT_PASSWORD", "Password1!")
    @patch("pipelines.aws_redshift_pipeline.REDSHIFT_PORT", 5439)
    @patch("pipelines.aws_redshift_pipeline.AWS_ACCESS_KEY", None)
    @patch("pipelines.aws_redshift_pipeline.AWS_SECRET_KEY", None)
    def test_create_table_executes_ddl(self):
        """create_table should open a connection and execute the DDL."""
        from pipelines.aws_redshift_pipeline import CREATE_TABLE_SQL, create_table

        client = boto3.client("redshift", region_name="eu-west-2")
        _create_redshift_cluster(client)

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("pipelines.aws_redshift_pipeline.psycopg2.connect", return_value=mock_conn):
            create_table()

        mock_cursor.execute.assert_called_once_with(CREATE_TABLE_SQL)

    @patch("pipelines.aws_redshift_pipeline.REDSHIFT_CLUSTER", CLUSTER_ID)
    @patch("pipelines.aws_redshift_pipeline.REDSHIFT_DATABASE", DATABASE)
    @patch("pipelines.aws_redshift_pipeline.REDSHIFT_USER", "admin")
    @patch("pipelines.aws_redshift_pipeline.REDSHIFT_PASSWORD", "Password1!")
    @patch("pipelines.aws_redshift_pipeline.REDSHIFT_PORT", 5439)
    @patch("pipelines.aws_redshift_pipeline.AWS_ACCESS_KEY", None)
    @patch("pipelines.aws_redshift_pipeline.AWS_SECRET_KEY", None)
    def test_connection_is_always_closed(self):
        """psycopg2 connection must be closed even if the DDL raises."""
        from pipelines.aws_redshift_pipeline import create_table

        client = boto3.client("redshift", region_name="eu-west-2")
        _create_redshift_cluster(client)

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("DDL failed")
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("pipelines.aws_redshift_pipeline.psycopg2.connect", return_value=mock_conn):
            with pytest.raises(Exception, match="DDL failed"):
                create_table()

        mock_conn.close.assert_called_once()


# ---------------------------------------------------------------------------
# copy_from_s3
# ---------------------------------------------------------------------------

@mock_aws
class TestCopyFromS3:
    @patch("pipelines.aws_redshift_pipeline.REDSHIFT_CLUSTER", CLUSTER_ID)
    @patch("pipelines.aws_redshift_pipeline.REDSHIFT_DATABASE", DATABASE)
    @patch("pipelines.aws_redshift_pipeline.REDSHIFT_USER", "admin")
    @patch("pipelines.aws_redshift_pipeline.REDSHIFT_PASSWORD", "Password1!")
    @patch("pipelines.aws_redshift_pipeline.REDSHIFT_PORT", 5439)
    @patch("pipelines.aws_redshift_pipeline.S3_BUCKET_TRANSFORMED", BUCKET)
    @patch("pipelines.aws_redshift_pipeline.REDSHIFT_IAM_ROLE", IAM_ROLE)
    @patch("pipelines.aws_redshift_pipeline.AWS_ACCESS_KEY", None)
    @patch("pipelines.aws_redshift_pipeline.AWS_SECRET_KEY", None)
    def test_copy_sql_references_correct_bucket(self):
        """COPY statement should reference the configured S3 bucket and role."""
        from pipelines.aws_redshift_pipeline import copy_from_s3

        client = boto3.client("redshift", region_name="eu-west-2")
        _create_redshift_cluster(client)

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch("pipelines.aws_redshift_pipeline.psycopg2.connect", return_value=mock_conn):
            copy_from_s3(s3_prefix="transformed/", bucket=BUCKET, iam_role=IAM_ROLE)

        executed_sql: str = mock_cursor.execute.call_args[0][0]
        assert BUCKET in executed_sql
        assert IAM_ROLE in executed_sql
        assert "COPY public.reddit_posts" in executed_sql
