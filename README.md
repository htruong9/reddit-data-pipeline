# Reddit Data Pipeline — AWS Stack

A comprehensive end-to-end data pipeline that extracts posts from the Reddit API,
orchestrates processing through Apache Airflow with Celery workers, stages raw data
in Amazon S3, catalogues and transforms it with AWS Glue and Amazon Athena, and loads
the final analytical dataset into Amazon Redshift.

## Architecture

```
Reddit API
    │
    ▼
┌──────────────────────────────────┐
│  Apache Airflow  (Docker)        │
│  ┌────────┐  ┌───────────────┐  │
│  │ Web UI │  │ Celery Worker │  │
│  └────────┘  └───────────────┘  │
│        │              │          │
│  ┌─────────────────────────┐    │
│  │  PostgreSQL (metadata)  │    │
│  └─────────────────────────┘    │
└──────────────┬───────────────────┘
               │  upload CSV / Parquet
               ▼
        ┌─────────────┐
        │  Amazon S3   │   (raw + transformed buckets)
        └──────┬──────┘
               │
       ┌───────┴────────┐
       ▼                ▼
┌────────────┐   ┌────────────┐
│  AWS Glue  │   │  AWS Glue  │
│  Crawler   │   │  ETL Job   │
└─────┬──────┘   └─────┬──────┘
      │                │
      ▼                ▼
┌────────────┐   ┌────────────────┐
│   Athena   │   │    Redshift     │
│  (ad-hoc)  │   │  (warehouse)   │
└────────────┘   └────────────────┘
```

## Components

| Component | Purpose |
|---|---|
| **Reddit API (PRAW)** | Source — extracts posts from any subreddit |
| **Apache Airflow** | Orchestration — schedules and monitors the DAG |
| **Celery + Redis** | Distributed task execution for Airflow workers |
| **PostgreSQL** | Airflow metadata DB and intermediate staging |
| **Amazon S3** | Raw and transformed data lake storage |
| **AWS Glue Crawler** | Automatic schema discovery and Data Catalogue |
| **AWS Glue ETL Job** | PySpark-based transformation |
| **Amazon Athena** | Serverless SQL queries over the data lake |
| **Amazon Redshift** | Columnar data warehouse for analytics |

## Prerequisites

* Python 3.9+
* Docker and Docker Compose
* An AWS account with permissions for S3, Glue, Athena, IAM, and Redshift
* Reddit API credentials (create an app at https://www.reddit.com/prefs/apps)

## Quick Start

1. **Clone and enter the repository**

   ```bash
   git clone https://github.com/htruong9/reddit-data-pipeline.git
   cd reddit-data-pipeline
   ```

2. **Create a virtual environment**

   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

3. **Configure credentials**

   ```bash
   cp config/config.conf.example config/config.conf
   # Edit config/config.conf with your Reddit API + AWS credentials
   ```

4. **Start all services**

   ```bash
   docker-compose up -d
   ```

5. **Open the Airflow UI**

   ```
   http://localhost:8080
   ```
   Default credentials: `airflow` / `airflow`

6. **Provision AWS resources**

   ```bash
   cd aws
   # Option A — Terraform
   terraform init && terraform apply

   # Option B — CloudFormation
   aws cloudformation deploy \
       --template-file cloudformation.yml \
       --stack-name reddit-pipeline \
       --capabilities CAPABILITY_NAMED_IAM
   ```

7. **Trigger the DAG** — enable `reddit_etl_pipeline` in the Airflow UI or run:

   ```bash
   docker exec -it airflow-webserver airflow dags trigger reddit_etl_pipeline
   ```

## Project Structure

```
reddit-data-pipeline/
├── dags/
│   └── reddit_dag.py              # Airflow DAG definition
├── etls/
│   ├── reddit_etl.py              # Extract from Reddit API
│   └── transform_etl.py           # Local pandas transforms
├── pipelines/
│   ├── aws_s3_pipeline.py         # Upload to S3
│   ├── aws_glue_pipeline.py       # Trigger Glue crawler + ETL job
│   └── aws_redshift_pipeline.py   # COPY into Redshift
├── utils/
│   ├── constants.py               # Shared config loader
│   └── helpers.py                 # Utility functions
├── config/
│   ├── config.conf.example        # Template configuration
│   └── config.conf                # Your actual credentials (git-ignored)
├── aws/
│   ├── cloudformation.yml         # CloudFormation IaC
│   ├── main.tf                    # Terraform IaC (alternative)
│   ├── variables.tf
│   ├── outputs.tf
│   └── glue_scripts/
│       └── transform_reddit.py    # PySpark script for Glue ETL job
├── tests/
│   ├── test_reddit_etl.py
│   ├── test_s3_pipeline.py
│   └── conftest.py
├── data/output/                   # Local CSV staging (git-ignored)
├── docker-compose.yml
├── Dockerfile
├── airflow.env
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

## Configuration

All credentials live in `config/config.conf`:

```ini
[reddit]
client_id = YOUR_REDDIT_CLIENT_ID
client_secret = YOUR_REDDIT_CLIENT_SECRET
user_agent = RedditDataPipeline/1.0
subreddit = dataengineering
post_limit = 100
time_filter = day

[aws]
access_key = YOUR_AWS_ACCESS_KEY
secret_key = YOUR_AWS_SECRET_KEY
region = eu-west-2
s3_bucket_raw = reddit-pipeline-raw
s3_bucket_transformed = reddit-pipeline-transformed
glue_database = reddit_db
glue_crawler = reddit_crawler
glue_job = reddit_transform_job
redshift_cluster = reddit-cluster
redshift_database = reddit_analytics
redshift_user = admin
redshift_password = YOUR_REDSHIFT_PASSWORD
redshift_port = 5439
redshift_iam_role = arn:aws:iam::ACCOUNT_ID:role/RedshiftS3ReadRole
```

## How it Works

1. **Extract** — the Airflow DAG triggers `reddit_etl.py`, which uses PRAW to pull
   posts from the configured subreddit. Fields include title, score, number of
   comments, author, URL, creation timestamp, and more.

2. **Stage locally** — extracted data is written as a timestamped CSV under
   `data/output/`.

3. **Upload to S3** — `aws_s3_pipeline.py` uploads the CSV to the raw S3 bucket,
   partitioned by date (`s3://bucket/raw/YYYY/MM/DD/`).

4. **Catalogue** — `aws_glue_pipeline.py` starts the Glue Crawler, which discovers
   the schema and registers it in the Glue Data Catalogue.

5. **Transform** — a Glue ETL job runs `transform_reddit.py` (PySpark), cleaning
   nulls, casting types, deduplicating, and writing Parquet to the transformed
   bucket.

6. **Query** — Athena is available immediately for ad-hoc SQL over the Parquet
   data lake.

7. **Load** — `aws_redshift_pipeline.py` issues a Redshift `COPY` command from the
   transformed bucket into the analytical table.

## Monitoring

* **Airflow UI** — task-level logs, retries, SLA misses
* **CloudWatch** — Glue job metrics, S3 request metrics, Redshift query performance
* **Athena Query History** — transformation audit trail

