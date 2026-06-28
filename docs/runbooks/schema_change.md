# Runbook: upstream payload schema change

**When to use**: GH Archive adds a field, removes a field, renames a
field, or changes a field's type. Detection usually comes from one
of:
- A `gate_silver` failure with "silver row count != bronze ..." after
  a successful Bronze ingest (the new field broke a `get_json_object`)
- A `dbt run` failure with `on_schema_change='fail'` (the new column
  changed the SELECT shape)
- An eye-bleed-level review of the postmortem
  [0001-schema-drift.md](../postmortems/0001-schema-drift.md)

This runbook IS the response. ADR-0001 already explains why we
designed Bronze to absorb these changes (raw JSON `payload_raw` +
narrow `payload_probe` struct) — most schema drift never reaches
Silver. The case this runbook addresses is when the drift DOES reach
Silver, i.e. when the field a Silver model parses changes.

## Steps

### 1. Confirm the breaking change is real, not a flake

Read the failing log and identify:
- Which Silver model failed (or which Gold gate)
- Which field name appears in the error

```bash
# Re-read the bronze rows for the suspect ingest_hour as raw JSON
JAVA_HOME=... uv run python -c "
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F
spark = configure_spark_with_delta_pip(
    SparkSession.builder.master('local[*]')
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
).getOrCreate()
df = (spark.read.format('delta').load('data/bronze/events')
      .filter('ingest_hour = \"2025-01-15-12\"')
      .filter(\"type = 'PushEvent'\")
      .select('payload_raw').limit(3))
for r in df.collect():
    print(r['payload_raw'])
" | python -m json.tool | head -50
```

Compare the field names you see to what the Silver model expects.

### 2. Categorize the change

| Category | Example | Severity |
|----------|---------|----------|
| **New optional field appears** | `payload.commits[].verification` added | Low — Silver ignores by default |
| **Field renamed** | `payload.size` → `payload.commit_count` | Medium — Silver's `get_json_object` returns null silently |
| **Field type narrowed** | `pull_request.number` STRING → INT | Medium — cast may fail or coerce wrong |
| **Required field removed** | `payload.push_id` gone | High — Silver `not_null` test fails |
| **Whole event type appears** | `PullRequestReviewEvent` first seen 2025 | Low — Bronze accepts; only matters if a mart wants it |
| **Whole event type removed** | `DownloadEvent` removed 2012 | Low — Silver `where type=...` just returns empty |

### 3. Apply the right fix

**For "field renamed"** — most common case:

1. Update the affected Silver model's `get_json_object(..., '$.old')`
   to `'$.new'` plus a `coalesce(...)` fallback to read the old name
   for historic data:
   ```sql
   coalesce(
       cast(get_json_object(payload_raw, '$.commit_count') as int),
       cast(get_json_object(payload_raw, '$.size')        as int)
   ) as commit_size
   ```
2. Full-refresh the Silver model:
   `cd dbt && uv run dbt run --select silver.events_push --full-refresh`
3. Full-refresh any Gold mart that reads the renamed column.
4. Re-run cross-mart gate to confirm the two marts agree post-fix.
5. Update the affected Silver column's description in
   `_silver_schema.yml` to note both names accepted.

**For "field type narrowed"** — the cast in Silver may now silently
return null instead of erroring. Update the cast to match the new
type and full-refresh.

**For "required field removed"** — this is a real outage. Either
the metric the field powers must be marked nullable in the mart, or
the mart must be deprecated. Open an ADR amendment, don't silently
band-aid.

**For "whole new event type"** — only an issue if a Gold mart's
math assumes the old type set. Otherwise ignore.

### 4. Add a regression check

Add an expectation to `quality/checks.py` that would have caught
this. Example for a rename:

```python
def silver_push_size_or_commit_count_present(silver_push, bronze):
    """After payload.size → payload.commit_count rename, ensure no
    silently-null rows."""
    n = silver_push.filter(F.col("commit_size").isNull()).count()
    return CheckResult(
        name="events_push.commit_size has the value (size or commit_count)",
        passed=n == 0,
        details=f"null commit_size rows={n}",
    )
```

This is the single most important step. *Every* schema-change
incident must leave behind a check the next one would fail on.

### 5. Verification

```bash
# Full pipeline re-run with the fixed Silver model
uv run python -m quality.runner --layer bronze
cd dbt && uv run dbt run --select silver --full-refresh && cd ..
uv run python -m quality.runner --layer silver
cd dbt && uv run dbt run --select gold --full-refresh && cd ..
uv run python -m quality.runner --layer gold
uv run python -m quality.runner --layer cross_mart
cd dbt && uv run dbt test
```

All gates green. Update the ADR registry if the fix changed an
existing decision.

## Out of scope: schema migrations on Bronze itself

We do not migrate Bronze. The Bronze contract is "store raw JSON + a
narrow probe struct" — neither has columns that depend on payload
shape. If the *envelope* changes (e.g. GitHub renames `repo` to
`repository`), that's a different runbook and requires editing
`spark/jobs/bronze_ingest.py` plus full-refreshing Bronze. As of
2026-06-28 the envelope has been stable for 10+ years, per
ADR-0001's discovery evidence.
