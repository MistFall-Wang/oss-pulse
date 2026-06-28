# Cross-account IAM role: Databricks data-plane workers assume this
# role from Databricks's AWS account to read/write the lakehouse
# buckets. This is the standard Databricks-on-AWS instance-profile
# pattern.

data "aws_iam_policy_document" "databricks_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::${var.databricks_aws_account_id}:root"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "databricks_lakehouse" {
  name               = "oss-pulse-${var.environment}-databricks-lakehouse"
  assume_role_policy = data.aws_iam_policy_document.databricks_assume_role.json
}

data "aws_iam_policy_document" "lakehouse_access" {
  statement {
    sid    = "ListBuckets"
    effect = "Allow"
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation",
    ]
    resources = [
      aws_s3_bucket.bronze.arn,
      aws_s3_bucket.silver.arn,
      aws_s3_bucket.gold.arn,
    ]
  }

  statement {
    sid    = "ReadWriteObjects"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:GetObjectVersion",
    ]
    resources = [
      "${aws_s3_bucket.bronze.arn}/*",
      "${aws_s3_bucket.silver.arn}/*",
      "${aws_s3_bucket.gold.arn}/*",
    ]
  }

  statement {
    sid    = "UseKmsForLakehouse"
    effect = "Allow"
    actions = [
      "kms:Decrypt",
      "kms:GenerateDataKey",
      "kms:DescribeKey",
    ]
    resources = [aws_kms_key.lakehouse.arn]
  }
}

resource "aws_iam_role_policy" "lakehouse_access" {
  name   = "lakehouse-rw"
  role   = aws_iam_role.databricks_lakehouse.id
  policy = data.aws_iam_policy_document.lakehouse_access.json
}
