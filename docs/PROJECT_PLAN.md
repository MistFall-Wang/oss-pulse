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

### Sprint 3: Widen Silver, build marts 2 and 3 — DONE

**Deliverables produced** (Sprint 3a):

- ADR-0005 — Silver build strategy (tiered, demand-driven) + dbt
  adapter swap plan for Sprint 5a
- Silver: `events_pull_request`, `events_issues`,
  `events_issue_comment` — incremental merge on id, schema yml with
  not_null / accepted_values / unique tests (35/35 pass)
- Gold: `oss_health_mart` (`docs/marts/gold_oss_health_mart.md`)
  - Grain: `(repo_id, activity_date)`, composite-key invariant
  - 11 metrics: PR open/close/merge counts + avg merge latency,
    issue open/close + avg close latency, comment count + avg
    first-non-opener-response latency, unique_contributors across
    PR+issue+comment
  - `spark/jobs/gold_health_verify.py`: grain invariant holds
    (30,107 unique rows out of 30,107); busiest merged-PR row
    (repo 35890081 on 2025-01-15, 54 merges) matches silver
    recomputation to 1ms tolerance
  - 18/18 schema tests pass

**Deliverables produced** (Sprint 3b):

- ADR-0006 — Bot identification: Rule A (`[bot]` suffix) + Rule C
  (`dbt/seeds/known_bots.csv` allowlist), with event-level
  `is_app_event` as a separate signal (not folded into bot counts).
  Rejects original Rule B per Sprint 2.5 spike evidence.
- `dbt/seeds/known_bots.csv` — initial entry: `LombiqBot`
- `dbt/macros/is_bot.sql` — shared macro used by both Gold marts
- Silver: `events_watch`, `events_fork` (14/14 schema tests pass)
- Gold: `bot_vs_human_activity_mart`
  (`docs/marts/gold_bot_vs_human_activity_mart.md`)
  - Grain: `(repo_id, activity_date)`, composite invariant
  - 14 metrics: total/bot/human event counts, bot_event_share,
    per-event-class bot counts (push/pr/issue/comment/watch/fork),
    distinct bot/human actor counts, app_event_count
  - 27/27 schema tests pass
- `gold.repo_daily_activity.sql` — refactored to use the canonical
  `is_bot()` macro (replaces the inline `like '%[bot]'` from
  Sprint 2). Full-refreshed; 13/13 tests still pass.
- `spark/jobs/gold_bot_verify.py` — grain invariant + cross-mart
  reconciliation. The cross-mart check caught the divergence above
  and forced repo_daily_activity to adopt the canonical rule before
  Sprint 3b could ship. **0 mismatches after rebuild** across 162,719
  joined keys.

**Data state**:
- Silver tier 2: events_pull_request (38,141 rows),
  events_issues (11,786), events_issue_comment (27,989)
- Silver tier 3: events_watch (27,097), events_fork (7,062)
- Gold: repo_daily_activity (162,719 rows),
  oss_health_mart (30,107 rows),
  bot_vs_human_activity_mart (199,416 rows)
- Bot share distribution across 199,416 repo-days:
  - 65.6% (130,834) at 0% bot share — pure human
  - 32.0% (63,924) at 100% bot share — pure bot
  -  2.3% (4,658)  somewhere in between — the interesting tail

**Verified invariants**:
- All 6 new Silver tables: `unique(id)` enforced (mirrors Bronze
  ADR-0002 idempotency chain)
- All 2 new Gold marts: composite-grain invariant enforced both by
  `dbt_utils.unique_combination_of_columns` and runtime verifier
- Cross-mart: `repo_daily_activity.bot_push_count ==
  bot_vs_human_activity_mart.push_bot_count` for every joined key
  (162,719 keys, 0 mismatches)
- Cross-layer: `oss_health_mart.pr_merged_count` and
  `pr_avg_merge_latency_hours` match silver recomputation to 1ms
  on the busiest row

### Sprint 4: Data quality + orchestration — DONE

**Deliverables produced**:
- `quality/checks.py` + `quality/runner.py` — lightweight DQ-gate
  framework (intentionally *not* full Great Expectations, design
  trade-off documented in checks.py docstring). 17 checks across 4
  suites (bronze / silver / gold / cross_mart). Each suite exits
  non-zero on failure → drop-in BashOperator gating. All 17 PASS on
  current sample.
- `airflow/dags/oss_pulse_pipeline.py` — parameterized DAG:
  plan_ingest_range → ingest_bronze → gate_bronze → build_silver →
  gate_silver → build_gold → gate_gold → gate_cross_mart →
  dbt_test_all. Parse-validated under apache-airflow 2.10.4 with 0
  import errors; arbitrary-date backfill via params.start_hour /
  end_hour.
- `docs/runbooks/`: backfill, schema_change, data_missing,
  airflow_setup — all action-first with decision trees / per-symptom
  fix matrices / verification commands

**Senior-signal reinforced**: 4 (DQ gates) + 7 (operational docs).

### Sprint 5a: Cloud migration + IaC — code-complete (apply pending user AWS account)

**Deliverables produced**:
- `terraform/` — S3 (3 buckets, KMS-encrypted, public-access blocked,
  versioning + 60-day-to-IA lifecycle on Bronze per ADR-0007 cost
  math), IAM cross-account role for Databricks workers, KMS key
- `docs/runbooks/cloud_migration.md` — 9-step Sprint 5a runbook with
  pre-reqs, terraform apply + output capture, aws s3 sync of existing
  Bronze (preserves _delta_log), Databricks workspace setup, dbt
  profile prod target, expected adapter-diff resolution notes,
  row-count verification target (162,719 / 30,107 / 199,416 must
  match), and rollback path

**Status**: Terraform code is apply-ready. Actual `terraform apply`
and the Databricks signup are user-side steps that cannot be done
from this session.

### Sprint 5b: CI + perf report + ADR-0007 + postmortem — DONE

**Deliverables produced**:
- `.github/workflows/ci.yml`: 3 jobs — static (ruff check + format),
  unit-tests (pytest spark/tests on JDK 17), dbt-static (deps + parse
  + compile). Full dbt run on Databricks deferred to Sprint 5a (which
  is code-complete pending user signup).
- `docs/performance/sprint5b_tuning.md` — 5-dimension perf benchmark
  with **honest negative finding**: OPTIMIZE+ZORDER did NOT prune at
  our 4-partition scale; the apparent wall-clock speedup was
  JIT/cache, not data-skipping. Storage cost: pre-VACUUM doubled
  (465 MB → 931 MB, restored to 466 MB after VACUUM). The whole
  experiment promoted ADR-0009 (VACUUM cadence) from aspirational to
  required.
- `docs/adr/0007-bronze-storage-overhead.md` — Bronze on Delta is
  1.7× raw `.json.gz`; planning constants for cloud cost forecasts
  derived from real measurements.
- `docs/postmortems/0001-schema-drift.md` — full 5-Whys for a
  deliberately-injected `payload.size → payload.commit_count` rename.
  Detection chain documented: Bronze + gate_bronze + silver build +
  gate_silver all PASS; dbt test catches it at end-of-pipeline (200
  violating rows). Root cause = gate placement, not missing test.
  Fix: coalesce(size, commit_count) in events_push.sql + new
  `silver_commit_size_not_null` gate that moves detection upstream.
- `spark/jobs/perf_bench.py`, `perf_vacuum.py`, `incident_inject.py`
  — reproducible scripts.

**Senior signals reinforced**: 5 (perf tuning report) + 7
(postmortem + ADR).

### Sprint 6: Streaming MVP — DONE

**Deliverables produced**:
- `streaming/docker-compose.yml` — Redpanda v24.2.7 single broker on
  port 19094 (deliberately chosen to coexist with the user's other
  Docker projects)
- `streaming/replay.py` — kafka-python producer; replays one hour's
  PushEvents (181,221 messages from 2025-01-15-12 in ~43 s at
  ~4 k msg/s), keyed by `repo_id` so the same repo's events hash to
  the same partition
- `streaming/consumer.py` — Spark Structured Streaming with
  `availableNow` trigger + `foreachBatch` + Delta MERGE on `id`
  (exactly-once via MERGE idempotency, no separate offset tracking)
- `streaming/reconcile.py` — batch ↔ streaming row count + commits sum
  + set-difference on `id`; threshold < 0.01 %
- `streaming/README.md` — full demo runbook + result + design notes

**Reconciliation result on 2025-01-15-12**:

```
batch rows:        181,221
streaming rows:    181,221
row count delta:   +0 (0.0000%)
batch commits Σ:   576,167
streaming commits: 576,167
ids only in batch:    0
ids only in streaming: 0
```

**Zero divergence on 181,221 events.** Threshold 0.01 %; actual
0.0000 %. Senior signal 6 (batch + streaming story) demonstrated.

### Sprint 7-9: Streaming production-grade — backlog (optional)

Triggered only if a job-market signal or interviewer asks for depth.
Otherwise the time is better spent on interview prep / blog writing /
another portfolio piece.

If pursued: time-warped replay (configurable speed), watermark-based
late-event handling, continuous reconciliation with root-cause
categorization, ADR-0008 (streaming time semantics), ADR-0009
(OPTIMIZE/VACUUM cadence), real-dollar cost report.

### Sprint 7-9: Streaming production-grade (optional)

**Triggered only if** the job market signal or an interviewer asks
for depth. Otherwise leave as backlog and invest the time in
interview prep, blog writing, or another portfolio piece.

---

## ADR registry

| #    | Title                                                   | Status   |
|------|---------------------------------------------------------|----------|
| 0001 | Bronze payload handling                                 | Accepted |
| 0002 | event_id as the sole idempotency key                    | Accepted |
| 0003 | Partition Bronze by ingest_hour, ZORDER by created_at   | Accepted |
| 0004 | No surrogate keys; use GitHub source ids                | Accepted |
| 0005 | Silver build strategy + dbt adapter swap plan           | Accepted |
| 0006 | Bot identification rules (Rule A + Rule C + is_app_event) | Accepted |
| 0007 | Bronze storage overhead (1.7× raw .json.gz, planning constants) | Accepted |
| 0008 | Streaming time semantics                                | Sprint 7 (if pursued) |
| 0009 | OPTIMIZE / VACUUM cadence                               | Sprint 9 (if pursued; preview captured in Sprint 5b tuning report) |

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
