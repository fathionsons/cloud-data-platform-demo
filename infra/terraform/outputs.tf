output "data_lake_bucket_name" {
  description = "S3 bucket name for bronze/silver/gold data."
  value       = aws_s3_bucket.data_lake.bucket
}

output "pipeline_iam_user_name" {
  description = "IAM user intended for pipeline access."
  value       = aws_iam_user.pipeline_user.name
}

output "pipeline_policy_arn" {
  description = "IAM policy ARN attached to pipeline user."
  value       = aws_iam_policy.pipeline_access.arn
}

output "glue_database_name" {
  description = "Glue catalog database name when enabled."
  value       = var.create_glue_catalog ? aws_glue_catalog_database.analytics[0].name : null
}
