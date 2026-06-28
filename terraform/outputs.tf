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

# NOTE: the Databricks cross-account IAM role and the customer-managed
# KMS key were dropped from the initial apply because the bootstrapping
# IAM user (`de-portfolio-cli`) lacks iam:CreateRole / kms:TagResource.
# Re-enable iam.tf.disabled and the KMS resources in s3.tf, then re-
# apply, once those permissions are granted (or once a Databricks
# workspace exists and you're ready to wire it up).
