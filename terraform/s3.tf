locals {
  bronze_bucket = "oss-pulse-bronze-${var.environment}-${var.bucket_suffix}"
  silver_bucket = "oss-pulse-silver-${var.environment}-${var.bucket_suffix}"
  gold_bucket   = "oss-pulse-gold-${var.environment}-${var.bucket_suffix}"
}

# KMS key shared across all three layer buckets so a single key
# rotation covers everything. Separate buckets are kept (not one bucket
# with prefixes) so per-layer lifecycle policies and S3 access logging
# stay clean.

resource "aws_kms_key" "lakehouse" {
  description             = "OSS Pulse lakehouse at-rest encryption"
  deletion_window_in_days = 30
  enable_key_rotation     = true
}

resource "aws_kms_alias" "lakehouse" {
  name          = "alias/oss-pulse-${var.environment}"
  target_key_id = aws_kms_key.lakehouse.key_id
}

resource "aws_s3_bucket" "bronze" {
  bucket = local.bronze_bucket
}

resource "aws_s3_bucket" "silver" {
  bucket = local.silver_bucket
}

resource "aws_s3_bucket" "gold" {
  bucket = local.gold_bucket
}

# Enforce KMS-encryption-at-rest on every bucket.
resource "aws_s3_bucket_server_side_encryption_configuration" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.lakehouse.arn
      sse_algorithm     = "aws:kms"
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "silver" {
  bucket = aws_s3_bucket.silver.id
  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.lakehouse.arn
      sse_algorithm     = "aws:kms"
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "gold" {
  bucket = aws_s3_bucket.gold.id
  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.lakehouse.arn
      sse_algorithm     = "aws:kms"
    }
  }
}

# Block all public access on every bucket. Lakehouse data is internal
# only; never expose via S3 ACLs.
resource "aws_s3_bucket_public_access_block" "bronze" {
  bucket                  = aws_s3_bucket.bronze.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "silver" {
  bucket                  = aws_s3_bucket.silver.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "gold" {
  bucket                  = aws_s3_bucket.gold.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Versioning: ON for Bronze (source of truth) and Gold (serving layer
# where accidental delete is most damaging). Silver is rebuildable from
# Bronze so versioning is overhead without recovery value.
resource "aws_s3_bucket_versioning" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_versioning" "gold" {
  bucket = aws_s3_bucket.gold.id
  versioning_configuration { status = "Enabled" }
}

# Lifecycle: auto-transition old Bronze partitions to Standard-IA after
# 60 days (90% of queries hit the last 30 days in our sample). Saves
# ~40% on storage cost for the cold tail.
resource "aws_s3_bucket_lifecycle_configuration" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  rule {
    id     = "tier-cold-bronze-to-ia"
    status = "Enabled"
    filter { prefix = "events/" }
    transition {
      days          = 60
      storage_class = "STANDARD_IA"
    }
    noncurrent_version_expiration {
      noncurrent_days = 7
    }
  }
}
