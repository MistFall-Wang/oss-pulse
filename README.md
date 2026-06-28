# OSS Pulse

A production-grade GitHub activity lakehouse built on the
[GH Archive](https://www.gharchive.org/) dataset.

**Status**: Sprint 2 complete — Bronze → Silver → Gold end-to-end runs
locally on PySpark + Delta + dbt-spark.

Designed to demonstrate idempotent ingestion at scale, schema-drift
tolerance, end-to-end data quality gates, performance/cost tuning, and
batch-streaming reconciliation.

The full master plan is in
[`docs/PROJECT_PLAN.md`](docs/PROJECT_PLAN.md). Architectural decisions
are in [`docs/adr/`](docs/adr/).

## First business query (after Sprint 2)

The first Gold mart, `gold.repo_daily_activity`, gives one row per
`(repo_id, activity_date)` with push / commit / contributor counts and
a bot vs non-bot split. Below is the result of running it on the
Sprint 1 sample (4 ingest hours: 2015-01-15-12, 2018-01-15-12,
2025-01-15-12, 2025-01-15-13 — total 613,876 raw events).

### Top 10 multi-author repos on 2025-01-15 (≥ 5 unique pushers)

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

### What the same query without the `unique_pushers >= 5` filter shows

```text
| repo_name                  | push_count | unique_pushers | bot_push_count |
| frdpzk2/ppub               |       2672 |              1 |              0 |
| zacw-243L/How-to-...       |       1996 |              1 |              0 |
| brand22/d3                 |       1875 |              1 |              0 |
| CelestiaNFT/Welco...       |       1829 |              1 |              0 |
```

Single-actor accounts pushing ~2,000 times in one day. None of them are
caught by the current `[bot]`-suffix heuristic. This is exactly the
"uncertain bucket" finding from
[`docs/spikes/bot_heuristic.md`](docs/spikes/bot_heuristic.md), and
why ADR-0006 (Sprint 3) will add an event-level `is_app_event` flag
plus a curated allowlist instead of relying on the suffix alone.

## Layout

```
spark/         PySpark Bronze ingestion + verifiers
  jobs/        bronze_ingest.py, bronze_inspect.py, gold_verify.py,
               bot_heuristic_spike.py
  schemas.py   Bronze envelope schema (single source of truth)
dbt/           dbt-spark project (Silver + Gold)
  models/silver/   events_push.sql + schema yml
  models/gold/     repo_daily_activity.sql + schema yml
  macros/          register_external_sources, delta_source, generate_schema_name
data/
  raw/         GH Archive hourly .json.gz inputs
  bronze/      Delta-backed Bronze table
docs/
  PROJECT_PLAN.md
  adr/         ADR-0001..0004
  marts/       Per-Gold-mart design docs
  spikes/      Time-boxed validation experiments
  schema_discovery.md
  schema_drift_evidence.md
```

## Local dev

Requires Java 17 (Java 18+ breaks Spark 3.5 / Hadoop 3.3.4 via
`Subject.getSubject` removal). Tested on Amazon Corretto 17.

```bash
# Bronze ingestion
JAVA_HOME=/Library/Java/JavaVirtualMachines/amazon-corretto-17.jdk/Contents/Home \
  uv run python -m spark.jobs.bronze_ingest \
    --source data/raw/2025-01-15-12.json.gz \
    --bronze-path data/bronze/events

# Silver + Gold via dbt
cd dbt && JAVA_HOME=... uv run dbt deps
cd dbt && JAVA_HOME=... uv run dbt build

# Verify Gold mart invariant + ground truth
JAVA_HOME=... uv run python -m spark.jobs.gold_verify
```

## Per-Sprint progress

See [`docs/PROJECT_PLAN.md`](docs/PROJECT_PLAN.md) for full plan.
Current state: Sprint 2 complete (Gold mart 1 of 3 done).
