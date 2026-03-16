"""
AWS Glue ETL Job — Reddit Post Transformation
==============================================

This PySpark script is uploaded to S3 and referenced by the Glue Job
definition.  It reads raw CSV data from the raw bucket, applies
cleaning and type-casting, deduplicates, and writes Parquet to the
transformed bucket.

Glue job arguments (passed via ``--key value``)::

    --source_bucket     reddit-pipeline-raw
    --target_bucket     reddit-pipeline-transformed
    --glue_database     reddit_db
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    FloatType,
    IntegerType,
    StringType,
    TimestampType,
)

# -------------------------------------------------------------------
# Initialisation
# -------------------------------------------------------------------

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "source_bucket", "target_bucket", "glue_database"],
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

source_path = f"s3://{args['source_bucket']}/raw/"
target_path = f"s3://{args['target_bucket']}/transformed/"

# -------------------------------------------------------------------
# Read raw data
# -------------------------------------------------------------------

df = spark.read.option("header", "true").csv(source_path)

# -------------------------------------------------------------------
# Transformations
# -------------------------------------------------------------------

# Cast columns to correct types
df = (
    df.withColumn("score", F.col("score").cast(IntegerType()))
    .withColumn("num_comments", F.col("num_comments").cast(IntegerType()))
    .withColumn("created_utc", F.col("created_utc").cast(TimestampType()))
    .withColumn("over_18", F.col("over_18").cast(BooleanType()))
    .withColumn("edited", F.col("edited").cast(BooleanType()))
    .withColumn("spoiler", F.col("spoiler").cast(BooleanType()))
    .withColumn("stickied", F.col("stickied").cast(BooleanType()))
    .withColumn("upvote_ratio", F.col("upvote_ratio").cast(FloatType()))
    .withColumn("extracted_at", F.col("extracted_at").cast(TimestampType()))
)

# Fill nulls
df = df.fillna({"selftext": "", "author": "[deleted]"})

# Truncate very long text
df = df.withColumn("selftext", F.substring(F.col("selftext"), 1, 10000))
df = df.withColumn("title", F.substring(F.col("title"), 1, 1000))

# Deduplicate by post ID, keeping the most recently extracted version
df = df.orderBy(F.col("extracted_at").desc()).dropDuplicates(["id"])

# Add partition columns for efficient querying
df = (
    df.withColumn("year", F.year("created_utc"))
    .withColumn("month", F.month("created_utc"))
    .withColumn("day", F.dayofmonth("created_utc"))
)

# -------------------------------------------------------------------
# Write transformed data
# -------------------------------------------------------------------

df.write.mode("append").partitionBy("year", "month", "day").parquet(target_path)

job.commit()
