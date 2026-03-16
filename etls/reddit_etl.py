"""
Extract posts from the Reddit API using PRAW.

This module handles connection to Reddit, fetching posts from a configured
subreddit, and converting the raw submission objects into a pandas DataFrame
suitable for downstream processing.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import praw

from utils.constants import (
    POST_FIELDS,
    REDDIT_CLIENT_ID,
    REDDIT_CLIENT_SECRET,
    REDDIT_POST_LIMIT,
    REDDIT_SUBREDDIT,
    REDDIT_TIME_FILTER,
    REDDIT_USER_AGENT,
)

logger = logging.getLogger(__name__)


def _connect_reddit() -> praw.Reddit:
    """Return an authenticated, read-only PRAW Reddit instance."""
    reddit = praw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        user_agent=REDDIT_USER_AGENT,
    )
    logger.info("Connected to Reddit (read-only=%s)", reddit.read_only)
    return reddit


def _extract_post_data(post) -> dict | None:
    """Pull the fields we care about from a single PRAW Submission.

    Returns ``None`` if any field access raises (e.g. suspended accounts,
    deleted posts with restricted attributes) so the caller can skip it.
    """
    try:
        return {
            "id": post.id,
            "title": post.title,
            "selftext": post.selftext,
            "score": post.score,
            "num_comments": post.num_comments,
            "author": str(post.author) if post.author else "[deleted]",
            "url": post.url,
            "permalink": f"https://www.reddit.com{post.permalink}",
            "created_utc": datetime.fromtimestamp(post.created_utc, tz=timezone.utc).isoformat(),
            "over_18": post.over_18,
            "edited": bool(post.edited),
            "spoiler": post.spoiler,
            "stickied": post.stickied,
            "subreddit": str(post.subreddit),
            "upvote_ratio": post.upvote_ratio,
        }
    except Exception as exc:  # noqa: BLE001
        logger.warning("Skipping post %s — failed to extract: %s", getattr(post, "id", "?"), exc)
        return None


def extract_reddit_posts(
    subreddit: str | None = None,
    limit: int | None = None,
    time_filter: str | None = None,
) -> pd.DataFrame:
    """
    Fetch top posts from Reddit and return them as a DataFrame.

    Parameters
    ----------
    subreddit : str, optional
        Subreddit name (defaults to config value).
    limit : int, optional
        Maximum number of posts (defaults to config value).
    time_filter : str, optional
        One of ``hour``, ``day``, ``week``, ``month``, ``year``, ``all``.

    Returns
    -------
    pd.DataFrame
        A DataFrame with one row per post and columns matching ``POST_FIELDS``.
    """
    subreddit = subreddit or REDDIT_SUBREDDIT
    limit = limit or REDDIT_POST_LIMIT
    time_filter = time_filter or REDDIT_TIME_FILTER

    reddit = _connect_reddit()
    sub = reddit.subreddit(subreddit)

    logger.info(
        "Extracting up to %d posts from r/%s (time_filter=%s)",
        limit,
        subreddit,
        time_filter,
    )

    posts = []
    for submission in sub.top(time_filter=time_filter, limit=limit):
        data = _extract_post_data(submission)
        if data is not None:
            posts.append(data)

    df = pd.DataFrame(posts, columns=POST_FIELDS)

    # Basic cleaning
    df["selftext"] = df["selftext"].replace("", np.nan)
    df["created_utc"] = pd.to_datetime(df["created_utc"])

    logger.info("Extracted %d posts from r/%s", len(df), subreddit)
    return df
