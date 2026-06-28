variable "aws_region" {
  description = "AWS region for all OSS Pulse resources."
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment label appended to bucket names so dev / prod can coexist."
  type        = string
  default     = "dev"
  validation {
    condition     = contains(["dev", "stage", "prod"], var.environment)
    error_message = "environment must be one of: dev, stage, prod."
  }
}

variable "bucket_suffix" {
  description = "Random suffix appended to bucket names to make them globally unique. Generate once: openssl rand -hex 4."
  type        = string
}

variable "databricks_aws_account_id" {
  description = "Databricks's AWS account id (414351767826 for the commercial cloud). Used in the trust policy for the cross-account IAM role."
  type        = string
  default     = "414351767826"
}
