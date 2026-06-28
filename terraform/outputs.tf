output "bronze_bucket" {
  description = "S3 bucket for Bronze Delta tables. Use in dbt profile / Spark config as s3://<bucket>/events/."
  value       = aws_s3_bucket.bronze.bucket
}

output "silver_bucket" {
  description = "S3 bucket for Silver Delta tables."
  value       = aws_s3_bucket.silver.bucket
}

output "gold_bucket" {
  description = "S3 bucket for Gold Delta tables."
  value       = aws_s3_bucket.gold.bucket
}

output "databricks_lakehouse_role_arn" {
  description = "Cross-account IAM role for Databricks workers. Register this in the Databricks UI under 'Instance Profiles'."
  value       = aws_iam_role.databricks_lakehouse.arn
}

output "lakehouse_kms_key_arn" {
  description = "KMS key encrypting all lakehouse buckets."
  value       = aws_kms_key.lakehouse.arn
}
