# Walkthrough: applying the cloud migration end to end

Step-by-step guide to taking the lakehouse from local-only to running
on AWS S3 (and eventually Databricks). Newer / more verbose than
[`cloud_migration.md`](cloud_migration.md), which is the terse
runbook.

**Validated on 2026-06-28**: through step 6 (smoke test) on this
repo. Steps 7 onward depend on a Databricks Free Edition workspace
that the user signs up for.

## Cost preview (US East 1)

| Resource | Free tier | Cost above free |
|----------|-----------|-----------------|
| S3 storage, 3 buckets × ~450 MB total | first 5 GB free for 12 months | $0.023/GB-month |
| S3 requests | 20 k GET + 2 k PUT free | negligible |
| IAM role | always free | — |
| KMS customer-managed key | not in free tier | **$1/month per key** |
| Lifecycle / versioning / SSE | free | — |

**Total** while running: **~$0–1.20/month**. Down to **$0** as soon
as you `terraform destroy`.

> **Note on the initial apply**: the original Terraform also creates
> a customer-managed KMS key and a Databricks cross-account IAM role.
> Both require IAM permissions (`kms:CreateKey`, `iam:CreateRole`)
> that a least-privilege bootstrap user may not have. This
> walkthrough's defaults skip both — see step 9 for re-enabling them.

## Pre-reqs

| Item | How |
|------|-----|
| AWS account | [`aws.amazon.com`](https://aws.amazon.com); free tier covers Sprint-scale Bronze for the first 12 months |
| AWS CLI configured | `aws configure` — confirm with `aws sts get-caller-identity` |
| Terraform ≥ 1.6 | `brew tap hashicorp/tap && brew install hashicorp/tap/terraform`. **Note**: Terraform is no longer in homebrew/core; you need HashiCorp's tap. |
| Java 17 | Required by Spark 3.5; install Amazon Corretto 17 if not present |
| `boto3` in the venv | already a dep — `uv sync` brings it in |

## Step 1 — Generate a bucket suffix

S3 bucket names are global. The Terraform appends a random 8-char
hex suffix so multiple environments / re-applies don't collide.

```bash
cd terraform
openssl rand -hex 4
# e.g. 9f3eb8a5
```

## Step 2 — Write `terraform.tfvars`

```bash
cp terraform.tfvars.example terraform.tfvars
# edit terraform.tfvars and paste the hex from step 1
```

Resulting file (gitignored):

```hcl
aws_region    = "us-east-1"
environment   = "dev"
bucket_suffix = "9f3eb8a5"
```

## Step 3 — `terraform init`

```bash
terraform init
```

Downloads the AWS provider. Takes ~30 seconds the first time.

## Step 4 — `terraform plan`

```bash
terraform plan -out=tfplan
```

You should see **16 resources to add, 0 to change, 0 to destroy**:
3 S3 buckets, 3 SSE-KMS configs, 3 public-access blocks, 2 versioning
configs, 1 lifecycle config, 1 KMS key, 1 KMS alias, 1 IAM role,
1 IAM role policy.

> **If your IAM user lacks `iam:CreateRole` or `kms:CreateKey`**:
> See step 9.0 for the simplified apply that drops those resources.
> The buckets alone are enough to run Bronze on S3.

## Step 5 — `terraform apply`

```bash
terraform apply tfplan
```

Watch the resources create. Ends with the outputs:

```
bronze_bucket = "oss-pulse-bronze-dev-9f3eb8a5"
silver_bucket = "oss-pulse-silver-dev-9f3eb8a5"
gold_bucket   = "oss-pulse-gold-dev-9f3eb8a5"
databricks_lakehouse_role_arn = "arn:aws:iam::.../oss-pulse-dev-databricks-lakehouse"
lakehouse_kms_key_arn         = "arn:aws:kms:..."
```

Save these — step 7 needs them.

## Step 6 — Upload local Bronze to S3 + smoke-test

```bash
cd ..    # back to repo root
BRONZE=$(cd terraform && terraform output -raw bronze_bucket)

# Upload preserves _delta_log — needed for the table to be readable as Delta.
aws s3 sync data/bronze/events s3://$BRONZE/events/ --exact-timestamps

# Sanity check
aws s3 ls s3://$BRONZE/events/ --recursive | wc -l        # expect ~28 objects
aws s3 ls s3://$BRONZE/events/_delta_log/ | head           # expect commit jsons
```

Then run the **smoke test**: read the cloud Bronze via local Spark
and confirm the ADR-0002 idempotency invariant still holds.

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/amazon-corretto-17.jdk/Contents/Home
export PATH="$JAVA_HOME/bin:$PATH"

uv run python -m spark.jobs.s3_smoke_test --bucket $BRONZE
```

Expected output:

```
========== S3 Bronze smoke test ==========
path:                 s3a://oss-pulse-bronze-dev-9f3eb8a5/events
total rows:           613,876
distinct ids:         613,876
invariant (ADR-0002): total == distinct → True

[breakdown] rows per ingest_hour:
+-------------+------+
|ingest_hour  |count |
+-------------+------+
|2015-01-15-12|21062 |
|2018-01-15-12|63463 |
|2025-01-15-12|270553|
|2025-01-15-13|258798|
+-------------+------+

[s3_smoke_test] PASS — cloud Bronze matches local Bronze.
```

**This is the Sprint 5a milestone.** Bronze is live on AWS, queryable,
and matches the local source byte-for-byte.

## Step 7 — Sign up for Databricks Free Edition

This is the only step I can't script for you — it's a web flow that
needs your email.

1. Go to [`databricks.com/try-databricks`](https://databricks.com/try-databricks).
2. Pick **AWS** and the same region as your Terraform (`us-east-1` by default).
3. Accept the trial T&Cs.
4. Wait ~5 min for the workspace to provision.
5. In the workspace UI, go to **User Settings → Developer → Access tokens → Generate**.
   Save the token; it goes into `~/.dbt/profiles.yml` next.

## Step 8 — Register the IAM role in Databricks

In the Databricks workspace UI:

1. **Admin Settings → Workspace settings → Instance Profiles → Add Instance Profile.**
2. Paste the role ARN from `terraform output databricks_lakehouse_role_arn`.
3. Click **Add**.

Then attach the instance profile to a SQL Warehouse:

- **SQL → SQL Warehouses → Create** (or edit existing)
- **Advanced options → Instance profile → select the one you just added**

## Step 9 — Add the `prod` dbt profile

```bash
uv add dbt-databricks
```

Append to `~/.dbt/profiles.yml`:

```yaml
oss_pulse_dbt:
  target: dev   # existing local target stays the default
  outputs:
    dev:
      # ... existing local dbt-spark config ...
    prod:
      type: databricks
      catalog: oss_pulse              # create in UI: Catalog Explorer → Create catalog
      schema: silver
      host: <workspace>.cloud.databricks.com
      http_path: /sql/1.0/warehouses/<warehouse_id>
      token: "{{ env_var('DATABRICKS_TOKEN') }}"
      threads: 8
```

Export the token before running:

```bash
export DATABRICKS_TOKEN=dapi...your_token...
```

## Step 9.0 — Simplified apply (no IAM, no KMS)

If your bootstrap IAM user can't create IAM roles or KMS keys (very
common with portfolio-grade least-privilege users), use this path:

1. Move `iam.tf` aside so Terraform ignores it:
   ```bash
   mv terraform/iam.tf terraform/iam.tf.disabled
   ```
2. Drop the KMS resources from `terraform/s3.tf` (the `aws_kms_key`,
   `aws_kms_alias`, and three `aws_s3_bucket_server_side_encryption_configuration`
   blocks). S3 will fall back to its default SSE-S3 encryption
   (AES256) — still encrypted at rest, just with an AWS-managed key
   instead of a customer-managed one.
3. Remove the `databricks_lakehouse_role_arn` and `lakehouse_kms_key_arn`
   outputs from `outputs.tf`.
4. Re-run `terraform plan && terraform apply`.

When you later have the IAM/KMS permissions (or you're ready to wire
Databricks), restore the files from git history:

```bash
git mv terraform/iam.tf.disabled terraform/iam.tf
git checkout HEAD~N -- terraform/s3.tf terraform/outputs.tf  # whichever commit had KMS
terraform apply
```

## Step 10 — First prod dbt run

```bash
cd dbt
BRONZE=$(cd ../terraform && terraform output -raw bronze_bucket)
uv run dbt deps --target prod
uv run dbt seed --target prod
uv run dbt run --target prod --vars "{bronze_events_path: 's3://${BRONZE}/events'}"
uv run dbt test --target prod
```

## Step 11 — Verify row counts match local

```bash
uv run dbt show --target prod --inline "select count(*) from {{ ref('repo_daily_activity') }}" --limit 1
uv run dbt show --target prod --inline "select count(*) from {{ ref('oss_health_mart') }}" --limit 1
uv run dbt show --target prod --inline "select count(*) from {{ ref('bot_vs_human_activity_mart') }}" --limit 1
```

Expect **162,719 / 30,107 / 199,416** — identical to local.

## Tear-down (when you're done)

```bash
# 1. Empty buckets (terraform destroy refuses non-empty buckets)
BRONZE=$(cd terraform && terraform output -raw bronze_bucket)
SILVER=$(cd terraform && terraform output -raw silver_bucket)
GOLD=$(cd terraform && terraform output -raw gold_bucket)
for B in $BRONZE $SILVER $GOLD; do
  aws s3 rm "s3://$B" --recursive
  # versioned buckets need explicit version deletion too
  aws s3api delete-objects --bucket "$B" \
    --delete "$(aws s3api list-object-versions --bucket "$B" \
                  --query='{Objects: Versions[].{Key:Key,VersionId:VersionId}}')" 2>/dev/null || true
done

# 2. Destroy Terraform-managed resources
cd terraform && terraform destroy
```

After destroy: cost goes to **$0/month**. Local Bronze + dbt warehouse
are untouched.

## Common failures

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| `AccessDenied: iam:CreateRole` | bootstrap IAM user lacks IAM perms | step 9.0 (drop IAM from .tf) or grant `iam:CreateRole` to the user |
| `AccessDenied: kms:TagResource` | bootstrap IAM user lacks KMS perms | step 9.0 (drop KMS) or grant `kms:*` to the user |
| `BucketAlreadyExists` | suffix collision | re-run step 1 with a new suffix |
| `[s3_smoke_test] ... ClassNotFoundException: delta.DefaultSource` | jars not on classpath at JVM startup | ensure `PYSPARK_SUBMIT_ARGS` is set in the same shell, NOT just SparkSession.config |
| `terraform destroy` stuck on bucket | versioned objects remain | run the version-aware delete in the tear-down section |
