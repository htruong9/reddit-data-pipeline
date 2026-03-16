terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

locals {
  name_suffix = var.environment
}

# ====================================================================
# S3 Buckets
# ====================================================================

resource "aws_s3_bucket" "raw" {
  bucket = "${var.raw_bucket_name}-${local.name_suffix}"
  tags   = { Environment = var.environment }
}

resource "aws_s3_bucket_lifecycle_configuration" "raw_lifecycle" {
  bucket = aws_s3_bucket.raw.id
  rule {
    id     = "archive-old-raw"
    status = "Enabled"
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }
}

resource "aws_s3_bucket" "transformed" {
  bucket = "${var.transformed_bucket_name}-${local.name_suffix}"
  tags   = { Environment = var.environment }
}

resource "aws_s3_bucket" "glue_scripts" {
  bucket = "${var.glue_scripts_bucket_name}-${local.name_suffix}"
  tags   = { Environment = var.environment }
}

# Upload the Glue ETL script
resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.glue_scripts.id
  key    = "scripts/transform_reddit.py"
  source = "${path.module}/glue_scripts/transform_reddit.py"
  etag   = filemd5("${path.module}/glue_scripts/transform_reddit.py")
}

# ====================================================================
# IAM — Glue Service Role
# ====================================================================

resource "aws_iam_role" "glue" {
  name = "RedditPipelineGlueRole-${local.name_suffix}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3" {
  name = "GlueS3Access"
  role = aws_iam_role.glue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
      Resource = [
        aws_s3_bucket.raw.arn,
        "${aws_s3_bucket.raw.arn}/*",
        aws_s3_bucket.transformed.arn,
        "${aws_s3_bucket.transformed.arn}/*",
        aws_s3_bucket.glue_scripts.arn,
        "${aws_s3_bucket.glue_scripts.arn}/*",
      ]
    }]
  })
}

# ====================================================================
# IAM — Redshift Service Role
# ====================================================================

resource "aws_iam_role" "redshift" {
  name = "RedditPipelineRedshiftRole-${local.name_suffix}"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "redshift.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "redshift_s3" {
  name = "RedshiftS3Read"
  role = aws_iam_role.redshift.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:ListBucket"]
      Resource = [
        aws_s3_bucket.transformed.arn,
        "${aws_s3_bucket.transformed.arn}/*",
      ]
    }]
  })
}

# ====================================================================
# Glue — Catalogue Database
# ====================================================================

resource "aws_glue_catalog_database" "reddit" {
  name = var.glue_database_name
}

# ====================================================================
# Glue — Crawler
# ====================================================================

resource "aws_glue_crawler" "reddit" {
  name          = "reddit-crawler-${local.name_suffix}"
  role          = aws_iam_role.glue.arn
  database_name = aws_glue_catalog_database.reddit.name

  s3_target {
    path = "s3://${aws_s3_bucket.raw.id}/raw/"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  schedule = "cron(30 6 * * ? *)"
}

# ====================================================================
# Glue — ETL Job
# ====================================================================

resource "aws_glue_job" "reddit_transform" {
  name     = "reddit-transform-job-${local.name_suffix}"
  role_arn = aws_iam_role.glue.arn

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.glue_scripts.id}/scripts/transform_reddit.py"
    python_version  = "3"
  }

  default_arguments = {
    "--source_bucket"        = aws_s3_bucket.raw.id
    "--target_bucket"        = aws_s3_bucket.transformed.id
    "--glue_database"        = aws_glue_catalog_database.reddit.name
    "--job-bookmark-option"  = "job-bookmark-enable"
  }

  glue_version      = "4.0"
  number_of_workers = 2
  worker_type       = "G.1X"
  timeout           = 60
}

# ====================================================================
# Athena — Workgroup
# ====================================================================

resource "aws_athena_workgroup" "reddit" {
  name  = "reddit-workgroup-${local.name_suffix}"
  state = "ENABLED"

  configuration {
    result_configuration {
      output_location = "s3://${aws_s3_bucket.transformed.id}/athena-results/"
    }
  }
}

# ====================================================================
# Redshift — Cluster
# ====================================================================

resource "aws_redshift_cluster" "reddit" {
  cluster_identifier = "reddit-cluster-${local.name_suffix}"
  database_name      = "reddit_analytics"
  master_username    = var.redshift_master_user
  master_password    = var.redshift_master_password
  node_type          = var.redshift_node_type
  number_of_nodes    = var.redshift_number_of_nodes
  cluster_type       = var.redshift_number_of_nodes == 1 ? "single-node" : "multi-node"

  iam_roles          = [aws_iam_role.redshift.arn]
  publicly_accessible = false
  encrypted          = true

  skip_final_snapshot = true

  tags = { Environment = var.environment }
}
