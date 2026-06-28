# OSS Pulse

A production-grade GitHub activity lakehouse built on the
[GH Archive](https://www.gharchive.org/) dataset.

**Status**: Sprint 6 complete — end-to-end medallion + DQ gates +
Airflow DAG + CI + perf tuning + deliberate-incident postmortem +
batch↔streaming reconciliation. Sprint 5a (cloud) is code-complete
pending the user's AWS/Databricks signup.

The full master plan lives in
[`docs/PROJECT_PLAN.md`](docs/PROJECT_PLAN.md). Architectural decisions
are in [`docs/adr/`](docs/adr/). Operational playbooks are in
[`docs/runbooks/`](docs/runbooks/).

## What's done

| Layer | Artifact | Rows / state |
|-------|----------|---------------|
| Bronze | `spark/jobs/bronze_ingest.py` + Delta partitioned by `ingest_hour` | 613,876 events across 4 ingest hours |
| Silver | 6 dbt-spark models (push, PR, issues, issue_comment, watch, fork) | 497,396 typed rows; 49 schema tests pass |
| Gold | 3 dbt-spark marts (`repo_daily_activity`, `oss_health_mart`, `bot_vs_human_activity_mart`) | 392,242 rows; 58 schema tests pass |
| DQ gates | `quality/runner.py` (4 suites, 18 checks including new incident-0001 regression gate) | all PASS |
| Orchestration | `airflow/dags/oss_pulse_pipeline.py` | parameterized for arbitrary backfill, parse-validated |
| CI | `.github/workflows/ci.yml` | ruff + pytest + dbt parse on every PR |
| Streaming MVP | `streaming/` (Redpanda + replay + Structured Streaming consumer + reconcile) | 181,221 events reconciled batch↔streaming with **0 row delta** |
| ADRs | 7 accepted (0001-0007) + 2 pending optional Sprint 7-9 | every irreversible decision is documented |
| Postmortem | [`docs/postmortems/0001-schema-drift.md`](docs/postmortems/0001-schema-drift.md) | 5 Whys on a deliberately-injected schema rename; identified gate-placement gap and added a new regression check |
| Cloud (5a) | `terraform/` + `docs/runbooks/cloud_migration.md` | code-complete; apply requires user AWS account |

## First business query (Sprint 2 example)

The first Gold mart, `gold.repo_daily_activity`, has one row per
`(repo_id, activity_date)`.

```sql
select repo_name, push_count, total_commits, unique_pushers, bot_push_count
from gold.repo_daily_activity
where activity_date = date '2025-01-15'
  and unique_pushers >= 5
order by push_count desc
limit 10;
```

| repo_name              | push_count | total_commits | unique_pushers | bot_push_count |
|------------------------|-----------:|--------------:|---------------:|---------------:|
| NexusAILab/cdn         |        131 |           131 |              5 |              0 |
| odoo-dev/odoo          |         75 |         2,428 |             49 |              0 |
| LucasOtw/SAE3_Sco...   |         69 |            84 |              7 |              0 |
| hmcts/cnp-flux-co...   |         38 |            42 |              5 |              0 |
| demisto/content        |         36 |           394 |             15 |              6 |
| ZrenKix/PROJ2024       |         35 |           236 |              6 |              0 |
| grafana/grafana        |         33 |           593 |             20 |              2 |
| Matteo-K/PACT          |         32 |            50 |              5 |              0 |
| deckhouse/deckhouse    |         30 |            34 |             13 |              0 |
| MarcusZ98/Racketeers   |         29 |           159 |              7 |              0 |

## Layout

```
spark/         PySpark jobs
  jobs/        bronze_ingest, bronze_inspect, gold_verify, gold_health_verify,
               gold_bot_verify, bot_heuristic_spike, perf_bench, perf_vacuum,
               incident_inject
  schemas.py   Bronze envelope schema (single source of truth)
quality/       lightweight DQ-gate framework
  checks.py    individual check functions
  runner.py    per-layer suite CLI (exits non-zero on fail → Airflow gating)
dbt/           dbt-spark project
  models/silver/   6 event-type tables + schema yml
  models/gold/     3 marts + schema yml
  macros/          register_external_sources, delta_source,
                   generate_schema_name, is_bot
  seeds/           known_bots.csv (Rule C allowlist, ADR-0006)
airflow/dags/  parameterized DAG
  oss_pulse_pipeline.py
streaming/     Sprint 6 MVP (Redpanda + Structured Streaming)
  docker-compose.yml, replay.py, consumer.py, reconcile.py
terraform/     Sprint 5a IaC (S3 + IAM + KMS)
data/          (gitignored)
  raw/         GH Archive .json.gz inputs
  bronze/      Delta-backed Bronze table
  streaming/   Sprint 6 silver_streaming table
docs/
  PROJECT_PLAN.md
  adr/         ADR-0001..0007
  marts/       per-mart design docs
  runbooks/    backfill, schema_change, data_missing, airflow_setup, cloud_migration
  postmortems/ 0001-schema-drift
  performance/ sprint5b_tuning.md + raw bench JSON
  spikes/      time-boxed validation experiments
.github/workflows/ ci.yml
```

## Local dev

Requires Java 17 (Java 18+ removes `Subject.getSubject`, which Spark
3.5 / Hadoop 3.3.4 still call). Tested on Amazon Corretto 17.

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/amazon-corretto-17.jdk/Contents/Home
export PATH=$JAVA_HOME/bin:$PATH
export PYSPARK_SUBMIT_ARGS="--driver-memory 4g pyspark-shell"

# End-to-end on one hour
uv run python -m spark.jobs.bronze_ingest --source data/raw/2025-01-15-12.json.gz --bronze-path data/bronze/events
uv run python -m quality.runner --layer bronze
cd dbt && uv run dbt deps && uv run dbt run --select silver && cd ..
uv run python -m quality.runner --layer silver
cd dbt && uv run dbt run --select gold && cd ..
uv run python -m quality.runner --layer gold
uv run python -m quality.runner --layer cross_mart
cd dbt && uv run dbt test

# Verifiers (cross-layer ground truth)
uv run python -m spark.jobs.gold_verify
uv run python -m spark.jobs.gold_health_verify
uv run python -m spark.jobs.gold_bot_verify

# Streaming MVP
docker-compose -f streaming/docker-compose.yml up -d
uv run python -m streaming.replay --source data/raw/2025-01-15-12.json.gz
uv run python -m streaming.consumer
uv run python -m streaming.reconcile --ingest-hour 2025-01-15-12
```

## Senior-signal scorecard

| # | Signal | Where it lives |
|---|--------|----------------|
| 1 | Idempotency | ADR-0002 + Bronze MERGE on event_id + Silver per-type MERGE + Gold composite-key MERGE + cross-layer count invariants in 3 verifier scripts |
| 2 | Backfill / replay | Airflow DAG params.start_hour / end_hour; runbooks/backfill.md |
| 3 | Schema-drift tolerance | ADR-0001 (raw payload in Bronze) + incident-0001 postmortem (where it tore through and what we did) |
| 4 | DQ gates | quality/ — 18 checks across 4 suites; gates on every Airflow task boundary |
| 5 | Perf tuning report | docs/performance/sprint5b_tuning.md — 5-dim before/after with an honest negative finding |
| 6 | Batch + streaming story | streaming/ — 181,221 events reconciled to 0 row delta |
| 7 | Operational docs | 7 ADRs + 5 runbooks + 1 postmortem + 3 mart design docs + 1 bot heuristic spike doc |
