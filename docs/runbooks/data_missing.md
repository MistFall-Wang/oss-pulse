# Runbook: "yesterday looks light"

**When to use**: a stakeholder reports — or a dashboard shows — that
the latest day's numbers in a Gold mart are unexpectedly low. The
question is whether this is a real low-activity day, a broken
ingest, or a stale build.

Goal of this runbook: bottom-up diagnosis from Gold → Silver → Bronze →
source, identifying the broken layer in O(minutes).

## Decision tree

```
  Gold mart looks low on date D
        │
        ▼
  1. Does Bronze have any rows for ingest_hour starting with D?
        ├── NO  → cause is upstream ingest. Go to §A.
        └── YES → go to 2.
        │
        ▼
  2. Does Silver row count for ingest_hour D match bronze.events filter
     where DATE(created_at)=D for each event type?
        ├── NO  → Silver build is stale or partially failed. Go to §B.
        └── YES → go to 3.
        │
        ▼
  3. Does Gold have rows for activity_date = D?
        ├── NO  → Gold incremental cutoff didn't pick up D. Go to §C.
        └── YES → numbers in Gold are correct; the day was actually
                  low-activity, OR a downstream presentation issue.
                  Verify with the cross-check below.
```

## §A — Bronze is missing the data

### Diagnose

```bash
JAVA_HOME=... uv run python -c "
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
spark = configure_spark_with_delta_pip(
    SparkSession.builder.master('local[*]')
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
).getOrCreate()
spark.read.format('delta').load('data/bronze/events') \\
    .groupBy('ingest_hour').count() \\
    .orderBy('ingest_hour').show(50, truncate=False)
"
```

Look for missing or unusually-small `ingest_hour` partitions for D.

### Possible causes

- **Airflow run failed mid-ingest** — check
  `airflow dags list-runs --dag-id oss_pulse_pipeline` for the run
  covering D. If `failed`, check task logs.
- **GH Archive source file 404** — check
  `https://data.gharchive.org/<D>-<HH>.json.gz`. Files for the last 2
  hours sometimes lag publication.
- **Bronze write succeeded but to wrong table** — `data/bronze/events`
  vs a path typo. Check `ls data/bronze/`.

### Fix

Trigger a backfill for the affected range — see
[backfill.md](backfill.md).

## §B — Silver row count doesn't match Bronze for D

### Diagnose

```bash
# Run the silver QA gate. Failed checks name the broken event type.
uv run python -m quality.runner --layer silver
```

Output looks like:

```
[FAIL] silver.events_push row count == bronze.events where type='PushEvent' — silver=120000, bronze_filtered=145000
```

### Possible causes

- **Silver build hasn't run since the new Bronze ingest** — re-run
  `cd dbt && uv run dbt run --select silver`.
- **Silver incremental cutoff stuck** — the `ingest_hour > max`
  predicate may have been satisfied early, then a later out-of-order
  ingest arrived. Solution: full-refresh that specific Silver model.
- **Silver model errored mid-run** — check `dbt/logs/dbt.log` for the
  last run.
- **A schema-change broke the Silver SELECT** — see
  [schema_change.md](schema_change.md).

### Fix

For the simplest case (build hasn't run):
```bash
cd dbt && uv run dbt run --select silver
uv run python -m quality.runner --layer silver
```

For incremental cutoff stuck:
```bash
cd dbt && uv run dbt run --select silver.events_<broken_type> --full-refresh
```

## §C — Gold doesn't have D yet

### Diagnose

```bash
JAVA_HOME=... uv run dbt show --inline "
  select max(activity_date), count(*)
  from {{ ref('repo_daily_activity') }}
" --limit 1
```

If `max(activity_date)` is before D, Gold is behind Silver.

### Possible causes

- **dbt run --select gold hasn't been triggered** for the run that
  ingested D — re-run it.
- **Late-arriving event** — a PushEvent at 23:59 UTC of D-1 landed
  in an ingest_hour processed in D's pipeline. The Gold incremental
  cutoff is `activity_date > max(activity_date) in target` (see
  `dbt/models/gold/repo_daily_activity.sql`), which would have moved
  past D-1 once D's data came in, so the late event is silently
  dropped. This is a known limitation documented in
  `docs/marts/gold_repo_daily_activity.md` (Sprint 2 design).
  - **Workaround until Sprint 4 DAG fixes this**: full-refresh the
    affected Gold mart.

### Fix

```bash
cd dbt && uv run dbt run --select gold
uv run python -m quality.runner --layer gold
uv run python -m quality.runner --layer cross_mart
```

## Final cross-check (proves the day really was low)

If all three layers agree but the day looks low, the day was just
quiet. Verify by comparing to recent days:

```bash
JAVA_HOME=... uv run dbt show --inline "
  select activity_date,
         sum(push_count) as total_pushes,
         count(distinct repo_id) as active_repos
  from {{ ref('repo_daily_activity') }}
  group by activity_date
  order by activity_date desc
" --limit 14
```

Two weeks of context usually makes "actually quiet day" obvious — e.g.
weekend, US holiday, GitHub-wide outage on that date.

## Postmortem trigger

If the same root cause appears twice across this runbook's history,
file a postmortem under `docs/postmortems/` even if neither incident
caused downstream impact. Two incidents = pattern, not coincidence.
