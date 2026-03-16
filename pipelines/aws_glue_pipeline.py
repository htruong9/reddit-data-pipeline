"""
Pipeline for managing AWS Glue resources.

Provides functions to start the Glue Crawler (schema discovery) and
to trigger the Glue ETL Job (PySpark transformation).
"""

from __future__ import annotations

import logging
import time

import boto3

from utils.constants import (
    AWS_ACCESS_KEY,
    AWS_REGION,
    AWS_SECRET_KEY,
    GLUE_CRAWLER,
    GLUE_DATABASE,
    GLUE_JOB,
)

logger = logging.getLogger(__name__)

_POLL_INTERVAL = 30  # seconds


def _get_glue_client():
    """Return a configured boto3 Glue client.

    Falls back to the standard credential chain (IAM role, env vars, etc.)
    when explicit keys are not configured.
    """
    return boto3.client(
        "glue",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY or None,
        aws_secret_access_key=AWS_SECRET_KEY or None,
    )


# -----------------------------------------------------------------------
# Crawler
# -----------------------------------------------------------------------

def _glue_tables_exist(database: str) -> bool:
    """Return True if at least one table is registered in *database*."""
    client = _get_glue_client()
    try:
        response = client.get_tables(DatabaseName=database)
        return len(response.get("TableList", [])) > 0
    except client.exceptions.EntityNotFoundException:
        return False


def should_run_crawler(database: str | None = None) -> bool:
    """Return True if the crawler needs to run.

    The crawler is skipped when Glue tables already exist in the target
    database, because it is designed for *schema discovery* rather than
    processing each daily load.  Pass ``force=True`` via the DAG's
    ``run_crawler`` param if you need to force a schema refresh.
    """
    database = database or GLUE_DATABASE
    if not _glue_tables_exist(database):
        logger.info("No Glue tables found in '%s' — crawler required.", database)
        return True
    logger.info(
        "Glue tables already exist in '%s' — skipping crawler.", database
    )
    return False


def start_crawler(
    crawler_name: str | None = None,
    wait: bool = True,
    force: bool = False,
) -> str:
    """
    Start the Glue Crawler and optionally block until it finishes.

    Parameters
    ----------
    crawler_name : str, optional
        Name of the Glue Crawler (defaults to config value).
    wait : bool
        If ``True``, poll until the crawler reaches a terminal state.
    force : bool
        If ``False`` (default), skip the crawler when Glue tables already
        exist.  Set to ``True`` to force a schema refresh.

    Returns
    -------
    str
        Final crawler state, or ``"SKIPPED"`` when bypassed.
    """
    crawler_name = crawler_name or GLUE_CRAWLER
    client = _get_glue_client()

    if not force and not should_run_crawler():
        return "SKIPPED"

    logger.info("Starting Glue Crawler: %s", crawler_name)
    client.start_crawler(Name=crawler_name)

    if not wait:
        return "STARTED"

    while True:
        time.sleep(_POLL_INTERVAL)
        response = client.get_crawler(Name=crawler_name)
        state = response["Crawler"]["State"]
        logger.info("Crawler %s — state: %s", crawler_name, state)

        if state == "READY":
            last_crawl = response["Crawler"].get("LastCrawl", {})
            status = last_crawl.get("Status", "UNKNOWN")
            logger.info("Crawler finished with status: %s", status)
            return status

        if state == "STOPPING":
            continue  # still winding down

    return state  # pragma: no cover



# -----------------------------------------------------------------------
# ETL Job
# -----------------------------------------------------------------------

def start_glue_job(
    job_name: str | None = None,
    arguments: dict | None = None,
    wait: bool = True,
) -> str:
    """
    Trigger a Glue ETL Job run and optionally block until it completes.

    Parameters
    ----------
    job_name : str, optional
        Glue Job name (defaults to config value).
    arguments : dict, optional
        Extra ``--key value`` arguments forwarded to the PySpark script.
    wait : bool
        If ``True``, poll until the job reaches a terminal state.

    Returns
    -------
    str
        Final job run state (e.g. ``SUCCEEDED``, ``FAILED``).
    """
    job_name = job_name or GLUE_JOB
    arguments = arguments or {}
    client = _get_glue_client()

    logger.info("Starting Glue Job: %s", job_name)
    response = client.start_job_run(JobName=job_name, Arguments=arguments)
    run_id = response["JobRunId"]
    logger.info("Job run ID: %s", run_id)

    if not wait:
        return "STARTED"

    terminal_states = {"SUCCEEDED", "FAILED", "STOPPED", "ERROR", "TIMEOUT"}

    while True:
        time.sleep(_POLL_INTERVAL)
        run = client.get_job_run(JobName=job_name, RunId=run_id)
        state = run["JobRun"]["JobRunState"]
        logger.info("Job %s run %s — state: %s", job_name, run_id, state)

        if state in terminal_states:
            if state != "SUCCEEDED":
                error = run["JobRun"].get("ErrorMessage", "No error message")
                logger.error("Glue Job failed: %s", error)
            return state

    return state  # pragma: no cover
