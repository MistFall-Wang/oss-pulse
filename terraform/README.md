# Terraform — OSS Pulse cloud infra

Manages the AWS resources OSS Pulse needs to migrate from local
filesystem + local Spark to S3 + Databricks (Sprint 5a).

## Scope

| Resource | Managed here? | Reason |
|----------|---------------|--------|
| S3 buckets (Bronze, Silver, Gold) | ✅ | data layer, ADR-0003 partition layout depends on a stable bucket |
| IAM role for Databricks → S3 | ✅ | needs to outlive workspace tear-downs |
| KMS key for at-rest encryption | ✅ | compliance-grade default |
| Databricks workspace | ❌ | Databricks Free Edition is provisioned via the web UI; the workspace pre-exists Terraform |
| Databricks notebooks, jobs, clusters | ❌ | tracked as code in this repo (dbt/, spark/), not state-managed by Terraform per ADR-0005's spirit |
| Snowflake (if used) | ✅ (commented stub) | future Sprint 9+ if we end up serving Gold from Snowflake |

## Pre-reqs

- `terraform >= 1.6` (`brew install terraform`)
- AWS CLI configured with credentials that can create S3 + IAM
- Databricks Free Edition workspace already created in the web UI;
  note the workspace URL + a personal access token

## First-time apply

```bash
cd terraform
terraform init
terraform plan -out=tfplan
# Review the plan carefully — you're creating S3 buckets that cost
# money once Bronze grows past the free tier
terraform apply tfplan
```

Outputs include the bucket names and IAM role ARN. Copy them into
`docs/runbooks/cloud_migration.md` step 4 (the dbt profile
`prod` target).

## Tear-down (full cleanup)

```bash
# 1. Empty the buckets first (terraform destroy won't delete non-empty
#    buckets by design)
aws s3 rm s3://oss-pulse-bronze-XXX --recursive
aws s3 rm s3://oss-pulse-silver-XXX --recursive
aws s3 rm s3://oss-pulse-gold-XXX --recursive

# 2. Destroy
terraform destroy
```
