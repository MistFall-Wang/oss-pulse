# OSS Pulse — Project Master Plan

> Canonical master plan. Any assistant picking up this project reads this
> file in full, then the ADR(s) relevant to the current Sprint, then acts.

## Identity

**OSS Pulse** — a production-grade GitHub activity lakehouse built on the
GH Archive dataset. End-to-end medallion architecture demonstrating
ingestion, schema-drift tolerance, idempotency, data quality, performance
tuning, and batch–streaming reconciliation.

**Owner**: Peter Wang
**Purpose**: portfolio project for Canadian Senior Data Engineer roles
**Quality bar**: 9/10 by end of Sprint 5b (interview-defensible). The
9.5/10 streaming work in Sprint 6+ is an option, not a commitment — see
"Honest scope note" below.

## North Star

The interviewer should be able to talk through this project for 45
minutes and hear, at every layer, *why this decision and not the obvious
alternative*. "I used Spark, Delta, dbt, Airflow, Snowflake" is mid
signal. "I chose Delta over Iceberg because…, partition by ingest_hour
because…, MERGE on event_id because…" is the senior signal.

## Honest scope note (read before estimating)

This is a part-time evening project. Realistic timeline:

| Milestone                 | Original estimate | Revised estimate |
|---------------------------|-------------------|------------------|
| 9/10 version (Sprint 5b)  | 6 weeks           | **8–10 weeks**   |
| 9.5/10 (full streaming)   | +4 weeks          | +3–4 weeks, optional |

The 9 → 9.5 marginal signal from streaming is real but not the largest
gap. The largest interview gap a portfolio project cannot close is "did
you actually run something in prod under load and survive an incident."
Sprint 5b's deliberately-induced incident + postmortem is the best
available proxy. Stream after that only if there's time and the job
market still wants it.

## Tech stack (converged)

| Layer            | Tool                                                       |
|------------------|------------------------------------------------------------|
| Storage          | Local filesystem (dev) → S3 (Sprint 5a)                    |
| Table format     | Delta Lake (primary), Parquet (raw payload only)           |
| Compute          | PySpark local (dev + perf tuning) → Databricks (Sprint 5a) |
| Orchestration    | Airflow (local Docker, optional Astronomer free tier)      |
| Transformation   | dbt-spark locally → **dbt-databricks** adapter on Sprint 5a |
| Serving          | Databricks SQL Warehouse; Snowflake free trial optional    |
| Data quality     | Great Expectations                                         |
| CI/CD            | GitHub Actions                                             |
| IaC              | Terraform (S3, IAM only; not Databricks-internal objects)  |
| Streaming        | Redpanda (Kafka API) + Spark Structured Streaming          |
| Lineage          | OpenLineage + Marquez — **Sprint 5b stretch**, drop if tight |

**Adapter decision (rationale)**: dbt-spark works locally on
PySpark/Hive metastore. On Databricks, switching to the
`dbt-databricks` adapter is the supported path (Unity Catalog support,
better SQL warehouse integration). To avoid late-Sprint surprises,
ADR-0005 records the swap point and the macro audit that needs to
happen before flipping the profile.

## Seven senior signals (cross-cutting)

Every Sprint must reinforce at least one. These are non-negotiable.

1. **Idempotency** — re-ingesting any source file produces no duplicate rows
2. **Backfill / replay** — arbitrary date ranges, stable results
3. **Schema-drift tolerance** — payload handled per event type, unknowns
   land in raw, pipeline never crashes on new fields
4. **Data quality gates** — GE checks block downstream propagation
5. **Performance tuning report** — five-dimensional before/after
   (data volume, wall clock, shuffle write, file count, cost)
6. **Batch + streaming story** — GH Archive replay to Kafka, Structured
   Streaming consumer, reconciliation against batch
7. **Operational docs** — ADRs, runbooks, postmortem

## Per-Sprint exit criteria (apply to every Sprint going forward)

A Sprint is not DONE until:

- New PySpark code has a chispa-based unit test
- New dbt models have schema tests (unique / not_null at minimum)
- At least one verifiable invariant for the layer touched (e.g.
  `count(*) == count(distinct id)`)
- The relevant ADR(s) for irreversible decisions are written
- This PROJECT_PLAN.md's Sprint section is updated with actual
  deliverables

## Gold marts (exactly three, deep not wide)

1. **Repo Growth Mart** — daily push/star/fork/PR/issue counts per repo,
   7-day anomaly detection
2. **OSS Health Mart** — commit cadence, unique contributors, issue
   response time, PR merge latency
3. **Bot vs Human Activity Mart** — bot identification rules, bot share
   of traffic, impact on top repos

Anything beyond these three is scope creep until Sprint 9+.

---

## Sprint roadmap

### Sprint 0: Schema discovery and decisions — DONE

**Deliverables produced**:
- `docs/schema_discovery.md` — 7 design questions answered with real data
- `docs/schema_drift_evidence.md` — cross-year (2015 / 2018 / 2025) drift evidence
- `docs/adr/0001-payload-handling.md`

**Key findings**:
- Top-level envelope schema stable 10 years (8 fields, same types)
- IssueCommentEvent nested paths grew 120 → 309 across decade
- New event type (PullRequestReviewEvent) appeared in 2025
- event_id 100% unique within and across hours (0 dupes in 613K combined sample)

### Sprint 1: Bronze + first Silver — DONE

**Deliverables produced**:
- `spark/jobs/bronze_ingest.py` — PySpark + Delta MERGE on event_id
- `spark/jobs/bronze_inspect.py` — table inspection utility
- `spark/schemas.py` — Bronze schema as single source of truth
- `dbt/macros/delta_source.sql` — abstraction wrapping source()
- `dbt/macros/register_external_sources.sql` — on-run-start hook
- `dbt/models/silver/events_push.sql` — incremental merge model
- `dbt/models/silver/_silver_schema.yml` — schema tests
- `docs/adr/0002-event-id-idempotency.md`
- `docs/adr/0003-partition-by-ingest-hour.md`

**Data state**:
- Bronze: 613,876 events across ingest_hours
  2015-01-15-12 (21,062), 2018-01-15-12 (63,463),
  2025-01-15-12 (270,553), 2025-01-15-13 (258,798)
- Silver: 385,321 PushEvents flattened

**Verified invariants**:
- Bronze: `count(*) == count(distinct id)` after every write
- Silver: same invariant via dbt `unique` test (1.77s on 385K rows)
- Both layers idempotent on re-run (zero new rows)
- Silver row count == Bronze rows where type = 'PushEvent'

### Sprint 2: First Gold mart — DONE

**Deliverables produced**:
- `dbt/packages.yml` — dbt-utils 1.4.0 installed, `dbt deps` clean
- `dbt/macros/generate_schema_name.sql` — schema-override macro so
  custom_schema names are used verbatim (`gold`, not `silver_gold`)
- `dbt/dbt_project.yml` — `silver` and `gold` schema configs added
- `docs/marts/gold_repo_daily_activity.md` — full design doc (grain,
  metrics, idempotency contract, known late-arrival limitation,
  ground-truth strategy)
- `dbt/models/gold/repo_daily_activity.sql` — incremental merge on
  `(repo_id, activity_date)`
- `dbt/models/gold/_gold_schema.yml` —
  `dbt_utils.unique_combination_of_columns` + not_null + expression
  tests on every metric (13 tests, all pass)
- `spark/jobs/gold_verify.py` — runtime invariant + cross-layer
  ground-truth check
- `docs/adr/0004-no-surrogate-keys.md` — codified the no-surrogate
  decision from how the mart uses `(repo_id, activity_date)` directly
- README — first business query example with two contrasting views

**Data state**:
- Gold: 162,719 rows across the 4 ingest_hours
- Mart grain invariant holds: `count(*) == count(distinct repo_id, activity_date)`
- 4-metric cross-layer match on the busiest row (frdpzk2/ppub on
  2025-01-15: 2,672 pushes, 2,672 commits, 1 pusher, 0 bot)
- 13/13 dbt tests pass

**Verified invariants**:
- Composite grain: enforced both by dbt test and `gold_verify.py`
- Cross-layer: gold metrics recomputed from `silver.events_push` for
  the busiest row match to the last commit/pusher
- `non_bot_push_count == push_count - bot_push_count` is a schema test

### Sprint 2.5: Bot heuristic spike — DONE

**Deliverables produced**:
- `spark/jobs/bot_heuristic_spike.py`
- `docs/spikes/bot_heuristic.md`

**Key findings (will shape ADR-0006 in Sprint 3)**:
- `payload.performed_via_github_app` is **not** at the payload root —
  it lives on sub-objects (`issue.*`, `comment.*`, `pull_request.*`,
  `review.*`). The originally proposed Rule B was always going to
  return 0 hits at root.
- Once fixed, Rule B adds only +40 events and +27 actors beyond Rule A
  — and the 27 actors are *humans using a GitHub App*, not bots. Rule
  B is the wrong abstraction for "is this actor a bot".
- Rule A (`actor.login ends with [bot]`) catches 7 of 8 visible bots
  in the top-20-by-event-count (87.5 %, below the 90 % threshold).
  The miss (`LombiqBot`) is fixable with a curated allowlist
  ("Rule C") rather than a broader pattern.
- A large block of high-volume non-`[bot]` accounts
  (`frdpzk2`, `zacw-243L`, `CelestiaNFT`, …) look like scripted /
  spam activity but no rule can classify them without external
  signal. The mart will categorize as `bot / human / uncertain`
  rather than collapsing unknowns into `human`.

**Outcome**: ADR-0006 (Sprint 3) will adopt Rule A + Rule C
(allowlist) + an event-level `is_app_event` flag derived from the
nested payload paths. Sprint 2's `repo_daily_activity` uses Rule A
only — PushEvent rarely involves apps in this sample.

### Sprint 2.5: Bot heuristic spike (1 day) — NEW

**Why this exists**: ADR-0006 (Sprint 3) will codify the rule
"bot = login ends with `[bot]` OR `payload.performed_via_github_app` is
non-null". If that rule's coverage is low (< 90% of the obvious bots
in real Bronze data), the Bot mart in Sprint 3 needs a different
heuristic. Better to find out now than to rewrite Sprint 3.

**Deliverables**:
- `spark/jobs/bot_heuristic_spike.py` — reads Bronze, reports:
  - Count of distinct actors with `[bot]` login suffix
  - Count of events where `payload.performed_via_github_app` is non-null
  - Overlap (events flagged by both rules vs only one)
  - Top 20 actors-by-event-count, manually labelled bot/human/uncertain
- `docs/spikes/bot_heuristic.md` — findings + ADR-0006 prognosis

**Pass condition**: union of the two rules catches ≥ 90 % of obviously-bot
top-event-count actors in the 2025 Bronze hours. Fail → ADR-0006 needs
a third rule or different approach.

### Sprint 3: Widen Silver, build marts 2 and 3

**Goal**: support the remaining Gold marts.

**Silver layer additions** (in order of demand):
- `events_pull_request` (for OSS Health)
- `events_issue_comment` (for OSS Health)
- `events_watch` (for Repo Growth: star events)
- `events_fork` (for Repo Growth: fork events)
- `events_issues` (for OSS Health)

**Gold layer additions**:
- `gold.oss_health_mart` — PR merge latency, issue response time,
  commit cadence per repo
- `gold.bot_vs_human_activity_mart` — bot share over time, per-repo
  bot impact (uses heuristic confirmed in Sprint 2.5)

**Companion ADRs**:
- ADR-0005 Silver build strategy (tiered, demand-driven) + dbt
  adapter swap plan
- ADR-0006 Bot identification rules (informed by Sprint 2.5)

**Sprint 3 done means**: all three Gold marts exist, each with
invariant validation against real Bronze data.

### Sprint 4: Data quality + orchestration

**Great Expectations integration**:
- 8–10 expectations split across Bronze and Silver:
  - Bronze: `id unique`, `id not null`, `type in {known types}`,
    `created_at not null`, `public = true`
  - Silver: per-mart not_null and range checks
- GE failures block downstream tasks via Airflow sensor

**Airflow DAG**:
- Tasks: download → bronze_ingest → run GE on bronze → dbt run silver →
  GE on silver → dbt run gold → GE on gold → dbt test all
- Parameterized for arbitrary date range backfill
- Idempotent: re-running the DAG produces no duplicates (relies on
  existing Bronze + Silver merge logic)

**Runbooks** (`docs/runbooks/`):
1. `backfill.md` — running an arbitrary date range
2. `schema_change.md` — what to do when upstream adds/removes a field
3. `data_missing.md` — diagnosis path from "yesterday looks light" to
   root cause

### Sprint 5a: Cloud migration + IaC

**Cloud migration**:
- Databricks Free Edition for compute (note: single-node, no job
  scheduling — see Sprint 5b perf-tuning note)
- S3 for Bronze storage
- Snowflake free trial as alternative Gold serving (optional)
- Swap `dbt-spark` → `dbt-databricks` adapter (per ADR-0005)

**IaC**:
- Terraform manages S3 buckets, IAM roles, and (if used) Snowflake
  databases / warehouses
- Terraform does NOT manage Databricks-internal objects (notebooks,
  jobs); those are tracked as code

**Done means**: full pipeline runs end-to-end on Databricks/S3,
results match local run on the same input.

### Sprint 5b: CI + perf report + postmortem

**CI/CD via GitHub Actions**:
- On every PR: `ruff`, `pytest` (chispa-based PySpark unit tests),
  `dbt compile`, `dbt parse`
- On merge to main: `dbt run --target prod`, `dbt test`

**Performance tuning report** (`docs/performance/sprint5_tuning.md`):
- Run **locally on multi-core PySpark**, not on Databricks Free
  Edition (single node = no meaningful tuning surface). Document this
  choice in the report so an interviewer asking "what cluster did you
  tune on" gets a defensible answer.
- Pick the slowest job (likely Bronze write or Silver events_push merge)
- Measure five dimensions: data volume, wall clock, shuffle write,
  file count, cost
- Apply tuning: Z-ORDER, partition pruning, broadcast join, file sizing
- Re-measure, document trade-offs

**ADR-0007**: storage overhead measurement (resolved by real Bronze data)

**Incident postmortem** (`docs/postmortems/0001-schema-drift.md`):
- Deliberately introduce a breaking schema change on a single hourly
  file (e.g. rename `payload.size` to `payload.commit_count`)
- Document the incident in 5 Whys format:
  detection → diagnosis → mitigation → recovery → preventive measures

**OpenLineage + Marquez** (stretch):
- Add OpenLineage emitters to Spark + dbt
- Render lineage in Marquez locally
- Drop if Sprint 5b is at risk of slipping; not interview-critical.

**End-of-Sprint 5b = the 9/10 version**. Resume-shippable.

### Sprint 6: Streaming MVP — minimum demo (≤ 1 week)

**Why a smaller MVP**: full streaming + reconciliation in 4 weeks
(original Sprint 6-9) is real engineering. Most Sr DE roles in Canada
will not probe streaming this deeply. Ship the demo, get interview
feedback, then decide.

**MVP deliverables**:
- Redpanda in Docker (1 broker)
- `streaming/replay.py` — replay 1 day of PushEvents from one Bronze
  hour to a `gh-events` topic, preserving event timestamps
- `streaming/consumer.py` — Structured Streaming consumer writes
  to `silver_streaming.events_push` (separate Delta path)
- `streaming/reconcile.py` — for that ingest_hour, batch silver vs
  streaming silver row counts differ by < 0.01 %

**Done means**: one talking-point demo. Code runs. Reconcile passes
once. No SLA, no production-grade.

### Sprint 7-9: Streaming production-grade (optional)

**Triggered only if** the job market signal or an interviewer asks
for depth. Otherwise leave as backlog and invest the time in
interview prep, blog writing, or another portfolio piece.

**Scope** (if pursued):
- Time-warped replay (configurable speed)
- Watermark-based late-event handling
- foreachBatch + Delta MERGE for streaming idempotency
- Continuous reconciliation with root-cause categorization of
  differences
- ADR-0008 streaming time semantics
- ADR-0009 OPTIMIZE / VACUUM cadence
- Real-dollar cost report

---

## ADR registry

| #    | Title                                              | Status                  |
|------|----------------------------------------------------|-------------------------|
| 0001 | Bronze payload handling                            | Accepted                |
| 0002 | event_id as the sole idempotency key               | Accepted                |
| 0003 | Partition Bronze by ingest_hour, ZORDER by created_at | Accepted             |
| 0004 | No surrogate keys; use GitHub source ids           | Sprint 2 (codify)       |
| 0005 | Silver build strategy + dbt adapter swap plan      | Sprint 3                |
| 0006 | Bot identification rules                           | Sprint 3 (informed by Sprint 2.5) |
| 0007 | Storage overhead measurement (Bronze)              | Sprint 5b               |
| 0008 | Streaming time semantics                           | Sprint 7 (if pursued)   |
| 0009 | OPTIMIZE / VACUUM cadence                          | Sprint 9 (if pursued)   |

ADR format: MADR-lite. See any existing ADR in `docs/adr/` for the template.
Status enum: Proposed / Accepted / Superseded.
Each ADR cites the code commit hash that implements (or codifies) it.

## Working principles

These are not rules. They are how Senior DEs actually work.

1. **Decide, then code** for irreversible choices (storage format,
   partition strategy, primary key). Write the ADR before or alongside
   the code.
2. **Code, then decide** for reversible choices (column names,
   materialization modes). Write the ADR after the fact to codify what
   works.
3. **Verifiable invariants beat verbal claims**. Every layer must have
   at least one invariant that a single command can verify
   (`count(*) == count(distinct id)`, row count matches upstream, etc).
4. **Names carry contracts**. `payload_probe` (not `payload`) tells
   future readers that the column is not a full typed representation.
5. **Open questions get owners and methods**, not just text. Bind every
   "TBD" to a specific Sprint and a specific verification approach.
6. **Reject over-engineering aloud**. If a decision rejects a textbook
   recommendation (e.g. no surrogate keys), the ADR must say why
   textbook doesn't apply here.
7. **Cross-year backfill is the design horizon**, not "what works for
   last month's data". Every schema decision answers: does this still
   work for 2015 data? for 2030 data?

## How to use this plan

- **Starting a new Sprint**: read this file, read the relevant ADRs
  cited in that Sprint's section, then start.
- **Mid-Sprint, stuck**: re-read the Sprint's stated goal and the
  senior signal(s) it's supposed to reinforce. If the work in front of
  you doesn't reinforce any signal, stop and ask whether it should be
  in this Sprint.
- **Finishing a Sprint**: update this file's Sprint section to mark
  DONE + list the actual deliverables produced. Then commit this file.

## What this project will NOT become

To prevent scope creep:

- Not a real-time dashboard for end users. Streaming is a *capability
  demonstration*, not a product.
- Not an ML project. Bot identification stays rule-based (ADR-0006).
- Not multi-source. Only GitHub data. Adding GitLab / Stack Overflow
  is a hypothetical Sprint 10+, not a real plan.
- Not multi-cloud. AWS-first. Azure / GCP equivalents are documented
  as alternatives in ADRs but not implemented.
