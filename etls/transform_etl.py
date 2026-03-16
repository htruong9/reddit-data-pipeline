"""
Local pandas-based transformations applied before uploading to S3.

These are lightweight cleaning steps.  Heavier transformations
(deduplication across runs, type casting to warehouse types, etc.)
happen in the AWS Glue PySpark job.
"""

from __future__ import annotations

import logging

import pandas as pd

logger = logging.getLogger(__name__)


def transform_reddit_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply cleaning and enrichment to the raw Reddit DataFrame.

    Steps
    -----
    1. Drop exact-duplicate rows (same ``id``).
    2. Fill missing ``selftext`` with empty string.
    3. Truncate very long self-text to 10 000 characters.
    4. Convert boolean columns to proper bool dtype.
    5. Add an ``extracted_at`` UTC timestamp column.

    Returns
    -------
    pd.DataFrame
        Cleaned copy of the input DataFrame.
    """
    logger.info("Transforming %d rows", len(df))

    df = df.copy()

    # 1. Deduplicate
    before = len(df)
    df = df.drop_duplicates(subset=["id"], keep="first")
    dropped = before - len(df)
    if dropped:
        logger.info("Dropped %d duplicate rows", dropped)

    # 2. Fill missing text
    df["selftext"] = df["selftext"].fillna("")

    # 3. Truncate
    max_len = 10_000
    df["selftext"] = df["selftext"].str[:max_len]

    # 4. Boolean columns
    bool_cols = ["over_18", "edited", "spoiler", "stickied"]
    for col in bool_cols:
        df[col] = df[col].astype(bool)

    # 5. Extraction timestamp
    df["extracted_at"] = pd.Timestamp.now("UTC")

    logger.info("Transformation complete — %d rows", len(df))
    return df
