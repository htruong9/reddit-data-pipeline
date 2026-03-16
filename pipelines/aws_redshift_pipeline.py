"""
Pipeline for loading transformed data into Amazon Redshift.

Uses the Redshift ``COPY`` command to bulk-load Parquet files from the
transformed S3 bucket.
"""

from __future__ import annotations

import logging

import boto3
import psycopg2

from utils.constants import (
    AWS_ACCESS_KEY,
    AWS_REGION,
    AWS_SECRET_KEY,
    REDSHIFT_CLUSTER,
    REDSHIFT_DATABASE,
    REDSHIFT_IAM_ROLE,
    REDSHIFT_PASSWORD,
    REDSHIFT_PORT,
    REDSHIFT_USER,
    S3_BUCKET_TRANSFORMED,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS public.reddit_posts (
    id              VARCHAR(20)   PRIMARY KEY,
    title           VARCHAR(1000) NOT NULL,
    selftext        VARCHAR(40000),
    score           INTEGER,
    num_comments    INTEGER,
    author          VARCHAR(200),
    url             VARCHAR(2000),
    permalink       VARCHAR(2000),
    created_utc     TIMESTAMP,
    over_18         BOOLEAN,
    edited          BOOLEAN,
    spoiler         BOOLEAN,
    stickied        BOOLEAN,
    subreddit       VARCHAR(200),
    upvote_ratio    FLOAT,
    extracted_at    TIMESTAMP
)
DISTSTYLE AUTO
SORTKEY (created_utc);
"""

COPY_SQL = """
COPY public.reddit_posts
FROM 's3://{bucket}/{prefix}'
IAM_ROLE '{iam_role}'
FORMAT AS PARQUET;
"""


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def _get_redshift_connection():
    """Return a psycopg2 connection to the Redshift cluster."""
    # Fetch the endpoint via the Redshift describe-clusters API so the
    # user does not need to hard-code it.
    client = boto3.client(
        "redshift",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY or None,
        aws_secret_access_key=AWS_SECRET_KEY or None,
    )
    cluster = client.describe_clusters(ClusterIdentifier=REDSHIFT_CLUSTER)
    endpoint = cluster["Clusters"][0]["Endpoint"]["Address"]

    conn = psycopg2.connect(
        host=endpoint,
        port=REDSHIFT_PORT,
        dbname=REDSHIFT_DATABASE,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD,
    )
    conn.autocommit = True
    return conn


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def create_table() -> None:
    """Ensure the ``reddit_posts`` table exists in Redshift."""
    logger.info("Creating reddit_posts table if it does not exist")
    conn = _get_redshift_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
        logger.info("Table ready")
    finally:
        conn.close()


def copy_from_s3(
    s3_prefix: str = "transformed/",
    bucket: str | None = None,
    iam_role: str | None = None,
) -> None:
    """
    Execute a ``COPY`` from S3 into the Redshift ``reddit_posts`` table.

    Parameters
    ----------
    s3_prefix : str
        Key prefix inside the bucket to load from.
    bucket : str, optional
        S3 bucket (defaults to ``S3_BUCKET_TRANSFORMED``).
    iam_role : str, optional
        IAM role ARN for the COPY command (defaults to config value).
    """
    bucket = bucket or S3_BUCKET_TRANSFORMED
    iam_role = iam_role or REDSHIFT_IAM_ROLE

    sql = COPY_SQL.format(bucket=bucket, prefix=s3_prefix, iam_role=iam_role)

    logger.info(
        "Running COPY from s3://%s/%s into Redshift", bucket, s3_prefix
    )
    conn = _get_redshift_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        logger.info("COPY complete")
    finally:
        conn.close()


def load_to_redshift(s3_prefix: str = "transformed/") -> None:
    """High-level wrapper: ensure table exists, then COPY data."""
    create_table()
    copy_from_s3(s3_prefix=s3_prefix)
