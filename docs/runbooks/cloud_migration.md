# Runbook: Sprint 5a cloud migration — local → S3 + Databricks

**When to use**: one-time migration from local filesystem + local
PySpark + dbt-spark (the Sprint 0-4 state) to S3 + Databricks Free
Edition + dbt-databricks. Not re-runnable.

**Time budget**: ~3 hours assuming Databricks signup + AWS account
ready. Free tier limits apply.

Per ADR-0005, the dbt adapter swaps from `dbt-spark` (session mode,
local Spark) to `dbt-databricks` (talks to a Databricks SQL warehouse
or cluster). The on-disk warehouse becomes the S3 buckets managed by
the Terraform in `terraform/`.

## Prerequisites

| Item | How to get it |
|------|---------------|
| AWS account | `aws.amazon.com` — free tier covers Sprint 4-scale Bronze for ~6 months |
| AWS CLI configured | `aws configure` — needs an IAM user with S3+IAM+KMS create perms |
| Terraform ≥ 1.6 | `brew install terraform` |
| Databricks Free Edition workspace | `databricks.com/try-databricks` — pick "AWS" + region matching your Terraform |
| Databricks personal access token | UI → Settings → User Settings → Tokens → New Token |

## Migration steps

### 1. Provision AWS infra with Terraform

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
# edit: set bucket_suffix to `openssl rand -hex 4` output
terraform init
terraform plan -out=tfplan
terraform apply tfplan
```

Save the outputs:

```bash
terraform output bronze_bucket
terraform output silver_bucket
terraform output gold_bucket
terraform output databricks_lakehouse_role_arn
```

### 2. Upload existing Bronze to S3

The Sprint 1-3 Bronze data was written locally. Copy it over so
historic incremental cutoffs still work.

```bash
BRONZE_BUCKET=$(cd terraform && terraform output -raw bronze_bucket)
aws s3 sync \
  /Users/peter/Desktop/oss-pulse/data/bronze/events \
  s3://$BRONZE_BUCKET/events/ \
  --exact-timestamps
```

Verify the `_delta_log` came along — losing it means the table is
unreadable as Delta.

```bash
aws s3 ls s3://$BRONZE_BUCKET/events/_delta_log/ | head
```

Silver and Gold are rebuildable from Bronze, so do NOT migrate them
— Sprint 5a re-builds them on Databricks.

### 3. Register the IAM role in Databricks

Databricks UI → Admin Settings → Workspace settings → Instance
Profiles → Add Instance Profile. Paste the role ARN from
`terraform output databricks_lakehouse_role_arn`.

Then attach the instance profile to your SQL warehouse / cluster:
- SQL Warehouse → Advanced options → Instance profile → select
- Or for an interactive cluster: Edit → Configuration → Instance
  profile

### 4. Install dbt-databricks alongside dbt-spark

```bash
uv add dbt-databricks
```

This adds the new adapter without removing dbt-spark — local dev
still works, and the new `prod` profile target uses Databricks.

### 5. Add the `prod` profile

Append to `~/.dbt/profiles.yml`:

```yaml
oss_pulse_dbt:
  target: dev   # existing local dev target stays the default
  outputs:
    dev:
      # ... existing local dbt-spark config ...
    prod:
      type: databricks
      catalog: oss_pulse        # Unity Catalog catalog (create in UI first)
      schema: silver            # base schema; gold/bronze are :+schema overrides
      host: <workspace>.cloud.databricks.com
      http_path: /sql/1.0/warehouses/<warehouse_id>
      token: "{{ env_var('DATABRICKS_TOKEN') }}"
      threads: 8
```

Export the token before running:
```bash
export DATABRICKS_TOKEN=dapi...your_token...
```

### 6. Migrate the bronze.events external location

Bronze on Databricks is an external table pointing at the S3 path.
Update the `register_external_sources` macro target to use S3 for
prod:

```sql
{# dbt/macros/register_external_sources.sql #}
{% macro register_external_sources() %}
    {% set bronze_path = var('bronze_events_path') %}
    {# vars override this per target via vars.yml or --vars CLI flag #}
    create schema if not exists bronze;
    create table if not exists bronze.events
      using delta
      location '{{ bronze_path }}';
{% endmacro %}
```

Set the path via `--vars` on the prod run:

```bash
BRONZE_BUCKET=$(cd terraform && terraform output -raw bronze_bucket)
cd dbt && uv run dbt run --target prod \
  --vars "{bronze_events_path: 's3://${BRONZE_BUCKET}/events'}" \
  --select silver gold
```

### 7. dbt parse against prod, fix adapter diffs

```bash
cd dbt && uv run dbt parse --target prod
```

Expected adapter differences (audit notes from ADR-0005):
- `+file_format: delta` — works on dbt-databricks unchanged
- `generate_schema_name` macro — works unchanged (returns custom name verbatim)
- `delta_source` macro — should work; uses `source()`
- Most likely surprise: MERGE syntax. dbt-spark uses
  `MERGE INTO ... WHEN MATCHED ...`; dbt-databricks supports the same
  with one additional `WHEN NOT MATCHED BY SOURCE` clause that
  dbt-spark didn't.

Resolve any errors one model at a time; commit each fix individually
so blame is clean for the future.

### 8. First full prod run

```bash
cd dbt
uv run dbt deps --target prod
uv run dbt seed --target prod
uv run dbt run --target prod --vars "{bronze_events_path: 's3://${BRONZE_BUCKET}/events'}"
uv run dbt test --target prod
```

Verify row counts match local:

```bash
uv run dbt show --target prod --inline "select count(*) from {{ ref('repo_daily_activity') }}" --limit 1
uv run dbt show --target prod --inline "select count(*) from {{ ref('oss_health_mart') }}" --limit 1
uv run dbt show --target prod --inline "select count(*) from {{ ref('bot_vs_human_activity_mart') }}" --limit 1
```

Should be 162,719 / 30,107 / 199,416 — same as local Sprint 3 state.

### 9. Wire CI to run prod-target tests on merge to main

Add a `.github/workflows/dbt-prod.yml` that:
- triggers on `push: branches: [main]`
- uses GitHub secrets for `DATABRICKS_TOKEN`, `AWS_ACCESS_KEY_ID`, etc.
- runs `dbt build --target prod` + `dbt test --target prod`

Keep the existing `ci.yml` (PR-time static checks) unchanged.

## Rollback

The local dev target keeps working throughout. To roll back the prod
migration:

1. Stop using `--target prod` in scripts
2. `terraform destroy` (after emptying buckets — see
   `terraform/README.md`)
3. The local warehouse at `dbt/spark-warehouse/` is untouched

No data is lost. The Bronze on S3 IS the same Bronze that was
locally produced; only the consumer changed.

## What this migration does NOT do

- It does not migrate to Unity Catalog as the *only* metastore — the
  local dev target stays on Hive metastore for offline work
- It does not parametrize Airflow to schedule the prod runs — that
  is Sprint 5b+ (move the DAG to Astronomer or Databricks Workflows)
- It does not enable cross-region replication — single-region per
  ADR-driven decision (region cost > cross-region reliability at
  portfolio scale)
