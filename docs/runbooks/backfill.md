# Runbook: backfill an arbitrary date range

**When to use**: re-process one or more GH Archive hours because
- you discovered a bug in a Silver/Gold model and need to rebuild,
- a previously-skipped hour needs to come in,
- the source file was updated upstream (GH Archive occasionally
  republishes).

**Idempotency contract** (relied on by this runbook):
- Bronze MERGE on `event_id` — re-ingesting an hour produces zero
  new rows (ADR-0002)
- Silver MERGE on `event_id` per type — same
- Gold MERGE on composite grain — same

You can re-run any range any number of times. The pipeline is
designed so backfills never duplicate.

## Steps

### 1. Decide the range

Range format is inclusive `YYYY-MM-DD-HH`. Examples:

| Goal | start_hour | end_hour |
|------|------------|----------|
| Just one hour | `2025-01-15-12` | `2025-01-15-12` |
| One full UTC day | `2025-01-15-00` | `2025-01-15-23` |
| Re-process 3 hours after a bug fix | `2025-01-15-12` | `2025-01-15-14` |

### 2. Trigger via Airflow

If the Airflow scheduler is running (see
[airflow_setup.md](airflow_setup.md)):

```bash
airflow dags trigger oss_pulse_pipeline \
  --conf '{"start_hour": "2025-01-15-12", "end_hour": "2025-01-15-14"}'
```

Then watch the run in the UI (http://localhost:8080) or:

```bash
airflow dags list-runs --dag-id oss_pulse_pipeline --output table
```

### 3. Or trigger manually (no Airflow)

For ad-hoc work, you can drive the same pipeline from the shell —
the DAG is just a thin wrapper around these commands:

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/amazon-corretto-17.jdk/Contents/Home
export PATH=$JAVA_HOME/bin:$PATH
export PYSPARK_SUBMIT_ARGS="--driver-memory 4g pyspark-shell"

for h in 2025-01-15-12 2025-01-15-13 2025-01-15-14; do
  # download if not cached
  [ -f data/raw/$h.json.gz ] || curl -sf -o data/raw/$h.json.gz https://data.gharchive.org/$h.json.gz
  # bronze
  uv run python -m spark.jobs.bronze_ingest --source data/raw/$h.json.gz --bronze-path data/bronze/events
done

uv run python -m quality.runner --layer bronze
cd dbt && uv run dbt run --select silver && cd ..
uv run python -m quality.runner --layer silver
cd dbt && uv run dbt run --select gold && cd ..
uv run python -m quality.runner --layer gold
uv run python -m quality.runner --layer cross_mart
cd dbt && uv run dbt test
```

### 4. Verification

After the run finishes, prove the backfill landed correctly:

```bash
# Bronze contains the hours you backfilled
JAVA_HOME=... uv run python -c "
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
spark = configure_spark_with_delta_pip(
    SparkSession.builder.master('local[*]')
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
).getOrCreate()
spark.read.format('delta').load('data/bronze/events').groupBy('ingest_hour').count().orderBy('ingest_hour').show(50)
"

# Gold marts reflect the new data — check expected new dates exist
JAVA_HOME=... uv run dbt show --inline "
  select activity_date, count(*) as repos from {{ ref('repo_daily_activity') }}
  group by activity_date order by activity_date
" --limit 20
```

### 5. If a backfill must REPLACE rather than MERGE

This is rare and risky. Typical reason: a Silver model's logic
changed and the historic rows are wrong, not just missing.

The right tool is dbt's `--full-refresh`:

```bash
cd dbt && uv run dbt run --select <model> --full-refresh
```

`--full-refresh` drops the target table and rebuilds it from all
upstream data, then re-runs every incremental cutoff from zero. This
DOES re-process the full Bronze, not just the range, so confirm
intent before running.

After a full refresh, re-run the cross-mart gate
(`quality.runner --layer cross_mart`) — full-refreshing one mart and
not the other is the most common way to introduce inconsistency.

## Common failures

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| `curl: (22) The requested URL returned error: 404` | hour not yet published, or future date | check `https://data.gharchive.org/` for availability |
| `JAVA_HOME` errors / `Subject.getSubject is not supported` | Java 18+ default | set `JAVA_HOME=.../amazon-corretto-17.jdk/...` |
| `OutOfMemoryError: Java heap space` on quality.runner | default driver heap too small | set `PYSPARK_SUBMIT_ARGS="--driver-memory 4g pyspark-shell"` |
| `gate_silver` fails with `silver row count != bronze` | a Bronze write succeeded but Silver MERGE didn't pick up the new ingest_hour | check `silver.events_<type>` for max(ingest_hour); compare to Bronze; full-refresh the affected silver model |
