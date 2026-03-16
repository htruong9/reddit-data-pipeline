output "raw_bucket_arn" {
  value = aws_s3_bucket.raw.arn
}

output "transformed_bucket_arn" {
  value = aws_s3_bucket.transformed.arn
}

output "glue_database_name" {
  value = aws_glue_catalog_database.reddit.name
}

output "redshift_endpoint" {
  value = aws_redshift_cluster.reddit.endpoint
}

output "redshift_iam_role_arn" {
  value = aws_iam_role.redshift.arn
}

output "athena_workgroup" {
  value = aws_athena_workgroup.reddit.name
}
