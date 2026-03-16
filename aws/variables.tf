variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "dev"
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "eu-west-2"
}

variable "raw_bucket_name" {
  description = "S3 bucket for raw Reddit data"
  type        = string
  default     = "reddit-pipeline-raw"
}

variable "transformed_bucket_name" {
  description = "S3 bucket for transformed data"
  type        = string
  default     = "reddit-pipeline-transformed"
}

variable "glue_scripts_bucket_name" {
  description = "S3 bucket for Glue ETL scripts"
  type        = string
  default     = "reddit-pipeline-glue-scripts"
}

variable "glue_database_name" {
  description = "Glue Data Catalogue database name"
  type        = string
  default     = "reddit_db"
}

variable "redshift_master_user" {
  description = "Redshift master username"
  type        = string
  default     = "admin"
  sensitive   = true
}

variable "redshift_master_password" {
  description = "Redshift master password"
  type        = string
  sensitive   = true
}

variable "redshift_node_type" {
  description = "Redshift node type"
  type        = string
  default     = "dc2.large"
}

variable "redshift_number_of_nodes" {
  description = "Number of Redshift nodes"
  type        = number
  default     = 1
}
