"""
Tests for etls/reddit_etl.py and etls/transform_etl.py.
"""

import numpy as np
import pandas as pd
import pytest

from etls.transform_etl import transform_reddit_data


class TestTransformRedditData:
    """Suite for the local pandas transformation step."""

    def test_deduplication(self, sample_reddit_df: pd.DataFrame):
        """Duplicate rows by ID should be removed."""
        duped = pd.concat([sample_reddit_df, sample_reddit_df.iloc[:1]], ignore_index=True)
        result = transform_reddit_data(duped)
        assert len(result) == len(sample_reddit_df)

    def test_selftext_null_filled(self, sample_reddit_df: pd.DataFrame):
        """NaN selftext values should become empty strings."""
        result = transform_reddit_data(sample_reddit_df)
        assert result["selftext"].isna().sum() == 0

    def test_boolean_columns(self, sample_reddit_df: pd.DataFrame):
        """Boolean columns should have bool dtype after transform."""
        result = transform_reddit_data(sample_reddit_df)
        for col in ("over_18", "edited", "spoiler", "stickied"):
            assert result[col].dtype == bool, f"{col} should be bool"

    def test_extracted_at_added(self, sample_reddit_df: pd.DataFrame):
        """A new ``extracted_at`` column should be present."""
        result = transform_reddit_data(sample_reddit_df)
        assert "extracted_at" in result.columns

    def test_selftext_truncation(self, sample_reddit_df: pd.DataFrame):
        """Selftext longer than 10 000 chars should be truncated."""
        df = sample_reddit_df.copy()
        df.loc[0, "selftext"] = "x" * 15_000
        result = transform_reddit_data(df)
        assert len(result.loc[0, "selftext"]) == 10_000

    def test_original_not_mutated(self, sample_reddit_df: pd.DataFrame):
        """The input DataFrame should not be changed in-place."""
        original_len = len(sample_reddit_df)
        _ = transform_reddit_data(sample_reddit_df)
        assert len(sample_reddit_df) == original_len
