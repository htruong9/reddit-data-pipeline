"""
Airflow DAG — Reddit ETL Pipeline
==================================

Schedule: daily at 06:00 UTC
Retries : 2 (5-minute delay)

Task graph::

    extract_reddit_data
           │
           ▼
    transform_data
           │
           ▼
    upload_to_s3
           │
           ▼
    run_glue_crawler          ← skipped automatically when schema already exists
           │
           ▼
    run_glue_etl_job
           │
           ▼
    load_to_redshift
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Make the project root importable inside the Airflow Docker container.
# The proper long-term fix is to install the project as a Python package
# (pip install -e .) in the container image so this sys.path manipulation
# is not needed.
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etls.reddit_etl import extract_reddit_posts
from etls.transform_etl import transform_reddit_data
from pipelines.aws_glue_pipeline import start_crawler, start_glue_job
from pipelines.aws_redshift_pipeline import load_to_redshift
from pipelines.aws_s3_pipeline import upload_csv_to_s3
from utils.constants import OUTPUT_DIR
from utils.helpers import utc_now_str

# -------------------------------------------------------------------
# Default args
# -------------------------------------------------------------------

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


# -------------------------------------------------------------------
# Task callables
# -------------------------------------------------------------------

def _extract(**context):
    """Extract posts, save locally, and push the local file path via XCom.

    We intentionally do *not* serialise the DataFrame into XCom because
    the Airflow metadata database is not designed to hold multi-MB blobs.
    Tasks communicate via file paths on the shared volume instead.
    """
    df = extract_reddit_posts()
    local_path = OUTPUT_DIR / f"reddit_raw_{utc_now_str()}.csv"
    df.to_csv(local_path, index=False)
    context["ti"].xcom_push(key="raw_csv", value=str(local_path))


def _transform(**context):
    """Read the raw CSV from disk, transform, and write a new file."""
    import pandas as pd

    raw_path = context["ti"].xcom_pull(key="raw_csv", task_ids="extract_reddit_data")
    # Read with explicit dtypes to avoid the silent type mangling that
    # happens when round-tripping through JSON (the previous approach).
    df = pd.read_csv(
        raw_path,
        parse_dates=["created_utc"],
        dtype={
            "over_18": bool,
            "edited": bool,
            "spoiler": bool,
            "stickied": bool,
        },
    )
    df = transform_reddit_data(df)
    transformed_path = OUTPUT_DIR / f"reddit_transformed_{utc_now_str()}.csv"
    df.to_csv(transformed_path, index=False)
    context["ti"].xcom_push(key="transformed_csv", value=str(transformed_path))


def _upload_s3(**context):
    """Upload the transformed CSV to S3 and push the S3 key via XCom."""
    transformed_path = context["ti"].xcom_pull(
        key="transformed_csv", task_ids="transform_data"
    )
    s3_key = upload_csv_to_s3(transformed_path)
    context["ti"].xcom_push(key="s3_key", value=s3_key)


def _run_crawler(**context):
    """Start the Glue Crawler only when schema discovery is needed."""
    status = start_crawler(wait=True)
    if status not in ("SUCCEEDED", "READY", "SKIPPED"):
        raise RuntimeError(f"Glue Crawler finished with status: {status}")


def _run_glue_job(**context):
    """Trigger the Glue ETL job and wait for completion."""
    state = start_glue_job(wait=True)
    if state != "SUCCEEDED":
        raise RuntimeError(f"Glue ETL Job finished with state: {state}")


def _load_redshift(**context):
    """COPY transformed Parquet data from S3 into Redshift."""
    load_to_redshift()


# -------------------------------------------------------------------
# DAG
# -------------------------------------------------------------------

with DAG(
    dag_id="reddit_etl_pipeline",
    default_args=default_args,
    description="End-to-end Reddit ETL: API → S3 → Glue → Athena → Redshift",
    schedule_interval="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["reddit", "etl", "aws"],
) as dag:

    extract = PythonOperator(
        task_id="extract_reddit_data",
        python_callable=_extract,
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=_transform,
    )

    upload_s3 = PythonOperator(
        task_id="upload_to_s3",
        python_callable=_upload_s3,
    )

    crawler = PythonOperator(
        task_id="run_glue_crawler",
        python_callable=_run_crawler,
    )

    glue_job = PythonOperator(
        task_id="run_glue_etl_job",
        python_callable=_run_glue_job,
    )

    redshift = PythonOperator(
        task_id="load_to_redshift",
        python_callable=_load_redshift,
    )

    extract >> transform >> upload_s3 >> crawler >> glue_job >> redshift
