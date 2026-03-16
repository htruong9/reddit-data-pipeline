"""
Shared configuration loader and constants for the Reddit data pipeline.
"""

import configparser
import os
from pathlib import Path


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = PROJECT_ROOT / "config" / "config.conf"
OUTPUT_DIR = PROJECT_ROOT / "data" / "output"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Config parser
# ---------------------------------------------------------------------------

_parser = configparser.ConfigParser()
_parser.read(CONFIG_PATH)


def _get(section: str, key: str, fallback: str | None = None) -> str | None:
    """Return a config value, preferring environment variables.

    Returns ``None`` (not an empty string) when no value is configured so
    boto3 clients can fall through to the standard credential chain
    (environment variables, IAM instance profile, etc.).
    """
    env_key = f"{section.upper()}_{key.upper()}"
    value = os.environ.get(env_key, _parser.get(section, key, fallback=fallback or ""))
    return value or None


# ---------------------------------------------------------------------------
# Reddit
# ---------------------------------------------------------------------------

REDDIT_CLIENT_ID = _get("reddit", "client_id")
REDDIT_CLIENT_SECRET = _get("reddit", "client_secret")
REDDIT_USER_AGENT = _get("reddit", "user_agent")
REDDIT_SUBREDDIT = _get("reddit", "subreddit")
REDDIT_POST_LIMIT = int(_get("reddit", "post_limit") or 100)
REDDIT_TIME_FILTER = _get("reddit", "time_filter") or "day"



# ---------------------------------------------------------------------------
# AWS
# ---------------------------------------------------------------------------

AWS_ACCESS_KEY = _get("aws", "access_key")
AWS_SECRET_KEY = _get("aws", "secret_key")
AWS_REGION = _get("aws", "region") or "eu-west-2"

S3_BUCKET_RAW = _get("aws", "s3_bucket_raw")
S3_BUCKET_TRANSFORMED = _get("aws", "s3_bucket_transformed")

GLUE_DATABASE = _get("aws", "glue_database")
GLUE_CRAWLER = _get("aws", "glue_crawler")
GLUE_JOB = _get("aws", "glue_job")

REDSHIFT_CLUSTER = _get("aws", "redshift_cluster")
REDSHIFT_DATABASE = _get("aws", "redshift_database")
REDSHIFT_USER = _get("aws", "redshift_user")
REDSHIFT_PASSWORD = _get("aws", "redshift_password")
REDSHIFT_PORT = int(_get("aws", "redshift_port") or 5439)
REDSHIFT_IAM_ROLE = _get("aws", "redshift_iam_role")

# ---------------------------------------------------------------------------
# Extraction fields
# ---------------------------------------------------------------------------

POST_FIELDS = [
    "id",
    "title",
    "selftext",
    "score",
    "num_comments",
    "author",
    "url",
    "permalink",
    "created_utc",
    "over_18",
    "edited",
    "spoiler",
    "stickied",
    "subreddit",
    "upvote_ratio",
]
