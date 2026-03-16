"""
Shared helper functions for the Reddit data pipeline.
"""

from datetime import datetime, timezone


def utc_now_str(fmt: str = "%Y%m%d_%H%M%S") -> str:
    """Return the current UTC timestamp as a formatted string."""
    return datetime.now(timezone.utc).strftime(fmt)


def s3_date_prefix() -> str:
    """Return a date-partitioned S3 key prefix, e.g. ``raw/2026/03/16/``."""
    now = datetime.now(timezone.utc)
    return f"raw/{now.year}/{now.month:02d}/{now.day:02d}/"


def safe_str(value) -> str:
    """Coerce *value* to a string, replacing None with an empty string."""
    if value is None:
        return ""
    return str(value)
