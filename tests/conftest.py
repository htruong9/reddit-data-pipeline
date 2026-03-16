"""
Shared pytest fixtures for the Reddit data pipeline test suite.
"""

import os

import pandas as pd
import pytest


@pytest.fixture()
def sample_reddit_df() -> pd.DataFrame:
    """Return a small DataFrame mimicking extracted Reddit posts."""
    return pd.DataFrame(
        {
            "id": ["abc123", "def456", "ghi789"],
            "title": [
                "First post title",
                "Second post title",
                "Third post title",
            ],
            "selftext": ["Some body text", None, ""],
            "score": [150, 42, 0],
            "num_comments": [30, 5, 0],
            "author": ["user_a", "[deleted]", "user_c"],
            "url": [
                "https://reddit.com/r/test/1",
                "https://reddit.com/r/test/2",
                "https://reddit.com/r/test/3",
            ],
            "permalink": [
                "https://www.reddit.com/r/test/1",
                "https://www.reddit.com/r/test/2",
                "https://www.reddit.com/r/test/3",
            ],
            "created_utc": pd.to_datetime(
                ["2026-03-15T10:00:00Z", "2026-03-15T11:00:00Z", "2026-03-15T12:00:00Z"]
            ),
            "over_18": [False, False, True],
            "edited": [False, True, False],
            "spoiler": [False, False, False],
            "stickied": [True, False, False],
            "subreddit": ["test", "test", "test"],
            "upvote_ratio": [0.95, 0.80, 0.50],
        }
    )


@pytest.fixture(autouse=True)
def _mock_aws_credentials(monkeypatch):
    """Ensure tests never hit real AWS by setting dummy credentials."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-west-2")
