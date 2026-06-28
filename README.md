<p align="center">
  <img src="docs/img/banner.svg" alt="OSS Pulse — a production-grade GitHub activity lakehouse" width="100%"/>
</p>

<p align="center">
  <a href="https://github.com/MistFall-Wang/oss-pulse/actions/workflows/ci.yml"><img src="https://github.com/MistFall-Wang/oss-pulse/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  &nbsp;
  <img src="https://img.shields.io/badge/reconcile%20delta-0.0000%25-22c55e" alt="reconcile delta 0">
  <img src="https://img.shields.io/badge/dbt%20tests-100%2B-22c55e" alt="100+ dbt tests">
  <img src="https://img.shields.io/badge/DQ%20gates-18-22c55e" alt="18 DQ gates">
  <img src="https://img.shields.io/badge/ADRs-7%20accepted-d97706" alt="7 ADRs">
  <img src="https://img.shields.io/badge/postmortems-1-d97706" alt="1 postmortem">
</p>

<p align="center">
  <b><a href="https://mistfall-wang.github.io/oss-pulse/">Visual showcase site</a></b>
  &nbsp;·&nbsp;
  <a href="docs/PROJECT_PLAN.md">Master plan</a>
  &nbsp;·&nbsp;
  <a href="docs/adr/">ADRs</a>
  &nbsp;·&nbsp;
  <a href="docs/runbooks/">Runbooks</a>
  &nbsp;·&nbsp;
  <a href="docs/postmortems/0001-schema-drift.md">Postmortem</a>
  &nbsp;·&nbsp;
  <a href="docs/runbooks/cloud_apply_walkthrough.md">Cloud apply walkthrough</a>
  &nbsp;·&nbsp;
  <a href="docs/video_demo_script.md">Video demo script</a>
</p>

---

End-to-end medallion on **Delta + dbt + Spark + Airflow**. 613,876 events
through Bronze → Silver → Gold, gated by **18 data-quality checks** at every
layer boundary. A deliberately-induced incident with a real postmortem, an
honest performance report that says "OPTIMIZE didn't help here, here's why",
and a streaming MVP that reconciled 181,221 events against batch with **zero
row delta**.

> [!TIP]
> Every irreversible decision in this project has an
> [ADR](docs/adr/). Every gate, postmortem, and verifier exists because it
> caught a real problem at least once. The visualizations below all use real
> measured numbers — nothing is illustrative.

## Results

<table align="center">
  <tr>
    <td align="center" width="20%">
      <h2><a href="docs/postmortems/0001-schema-drift.md">0.0000&nbsp;%</a></h2>
      <sub><b>batch ↔ streaming<br>reconcile delta</b><br>on 181,221 events</sub>
    </td>
    <td align="center" width="20%">
      <h2><a href="docs/adr/">7&nbsp;/&nbsp;9</a></h2>
      <sub><b>ADRs accepted</b><br>(2 optional in&nbsp;backlog)<br>every decision auditable</sub>
    </td>
    <td align="center" width="20%">
      <h2><a href="quality/runner.py">18</a></h2>
      <sub><b>data-quality gates</b><br>across 4 suites,<br>exit-coded for Airflow</sub>
    </td>
    <td align="center" width="20%">
      <h2><a href="docs/postmortems/0001-schema-drift.md">200</a></h2>
      <sub><b>rows of injected breakage</b><br>caught at the right gate<br>after the postmortem fix</sub>
    </td>
    <td align="center" width="20%">
      <h2><a href="docs/performance/sprint5b_tuning.md">−1.8&nbsp;s</a></h2>
      <sub><b>perf "win"</b><br>that I refused<br>(it was JIT, not ZORDER)</sub>
    </td>
  </tr>
</table>

## Tech stack

Every layer chosen with an alternative explicitly rejected. Versions pinned in `pyproject.toml`, `dbt/packages.yml`, `terraform/versions.tf`.

| Layer | Tool | Version | Why this, not the obvious alternative |
|-------|------|---------|----------------------------------------|
| **Compute** | ![Spark](https://img.shields.io/badge/-PySpark-e25a1c?logo=apachespark&logoColor=white) | 3.5 | Same code runs on Databricks at TB scale; Pandas was fine for 613K rows but not for the all-year backfill target |
| **Storage** | local filesystem (dev) | — | S3 swap is single `s3a://` URI; Terraform code for it is in `terraform/` (not deployed) |
| **Table format** | ![Delta](https://img.shields.io/badge/-Delta%20Lake-00add8) | 3.2.1 | MERGE syntax + dbt-spark adapter maturity; Iceberg revisit when Unity Catalog is needed |
| **Transformation** | ![dbt](https://img.shields.io/badge/-dbt--spark-ff694b?logo=dbt&logoColor=white) + dbt-utils | 1.9.2 / 1.4 | `ref()` / `source()` + declarative tests beat hand-managed model deps |
| **Orchestration** | ![Airflow](https://img.shields.io/badge/-Airflow-017cee?logo=apacheairflow&logoColor=white) | 2.10.4 | Standard Sr DE expectation; parameterized DAG via XCom-passed bash script |
| **Streaming** | ![Redpanda](https://img.shields.io/badge/-Redpanda-d97706) + Spark Structured Streaming | v24.2.7 | Kafka-API compatible, 2 s boot vs 30 s; consumer code unchanged |
| **Cloud IaC** | ![Terraform](https://img.shields.io/badge/-Terraform-7b42bc?logo=terraform&logoColor=white) | 1.15.7 | `.tf` for S3 + IAM + KMS authored & validated (`terraform plan`); apply step left for a real cloud deployment |
| **CI/CD** | ![GH Actions](https://img.shields.io/badge/-GitHub%20Actions-2088ff?logo=githubactions&logoColor=white) | — | 3 jobs: ruff · pytest · dbt parse+compile, JDK 17 |
| **Data quality** | custom Python (NOT Great Expectations) | — | 150 lines mirror GE's checkpoint pattern; trade-off [documented](quality/checks.py) |
| **Lang / runtime** | ![Python](https://img.shields.io/badge/-Python-3776ab?logo=python&logoColor=white) ![Java](https://img.shields.io/badge/-Java%2017-007396?logo=openjdk&logoColor=white) ![SQL](https://img.shields.io/badge/-SQL-336791) | 3.11 / 17 / — | Java 17 required (Spark 3.5 breaks on Java 18+) |
| **Package mgr** | ![uv](https://img.shields.io/badge/-uv-de5fe9) | 0.8 | Faster + reproducible-by-default lockfile vs pip |
| **Linter / formatter** | ![ruff](https://img.shields.io/badge/-ruff-d7ff64?logoColor=black) | latest | check + format check in CI |

## At a glance

|       Bronze events |  Delta tables |       dbt tests |        DQ gates | Accepted ADRs | Runbooks | Postmortems |        Reconcile delta |
| ------------------: | ------------: | --------------: | --------------: | ------------: | -------: | ----------: | ---------------------: |
|         **613,876** |        **11** |        **100+** |          **18** |         **7** |    **5** |       **1** |          **0.0000 %**  |

---

## Architecture

End-to-end medallion + a parallel streaming branch. Bronze stores `payload`
as raw JSON STRING so upstream schema drift never crashes ingestion
([ADR-0001](docs/adr/0001-payload-handling.md)). Every layer MERGEs on stable
GitHub ids — no surrogate keys ([ADR-0004](docs/adr/0004-no-surrogate-keys.md)).
Silver tables are built only when a Gold mart needs them
([ADR-0005](docs/adr/0005-silver-build-strategy.md)).

```mermaid
flowchart TD
    Raw[("raw .json.gz<br/>267 MB · 4 hours")]
    Bronze["<b>bronze.events</b><br/>Delta · partitioned by ingest_hour<br/>613,876 rows · 453 MB"]

    subgraph Silver ["<b>silver</b> — 6 tables, built on demand"]
        direction LR
        SPush["events_push<br/>385,321"]
        SPR["events_pull_request<br/>38,141"]
        SIC["events_issue_comment<br/>27,989"]
        SW["events_watch<br/>27,097"]
        SI["events_issues<br/>11,786"]
        SF["events_fork<br/>7,062"]
    end

    subgraph Gold ["<b>gold</b> — 3 marts"]
        direction LR
        GA["repo_daily_activity<br/>162,719"]
        GH["oss_health_mart<br/>30,107"]
        GB["bot_vs_human_activity_mart<br/>199,416"]
    end

    subgraph Stream ["streaming branch (Sprint 6 MVP)"]
        direction LR
        SR["replay.py<br/>→ Redpanda topic"]
        SC["Structured Streaming<br/>foreachBatch + Delta MERGE"]
        SX{{"silver_streaming.events_push<br/>0 row delta vs batch"}}
    end

    Raw --> Bronze
    Bronze --> SPush & SPR & SIC & SW & SI & SF
    SPush --> GA
    SPush --> GB
    SPR --> GH
    SPR --> GB
    SIC --> GH
    SIC --> GB
    SI --> GH
    SI --> GB
    SW --> GB
    SF --> GB

    Bronze -.->|replay one hour| SR
    SR --> SC --> SX

    classDef raw     fill:#f3f4f6,stroke:#6b7280,color:#1f2937
    classDef bronze  fill:#fdebd0,stroke:#cd7f32,color:#5d3a1a,font-weight:bold
    classDef silver  fill:#eceff1,stroke:#90a4ae,color:#263238
    classDef gold    fill:#fff4cc,stroke:#d97706,color:#5d3a05,font-weight:bold
    classDef stream  fill:#dbeafe,stroke:#2563eb,color:#1e3a8a

    class Raw raw
    class Bronze bronze
    class SPush,SPR,SIC,SW,SI,SF silver
    class GA,GH,GB gold
    class SR,SC,SX stream
```

### Event-type flow at scale — width = real row count

`PushEvent` is **two-thirds** of every event in the sample. The Sankey makes
the volume distribution visually obvious — and shows exactly how much each
Silver table costs to maintain.

```mermaid
---
config:
  sankey:
    showValues: false
    nodeAlignment: justify
---
sankey-beta

raw GH Archive (.json.gz),bronze.events,613876
bronze.events,events_push,385321
bronze.events,events_pull_request,38141
bronze.events,events_issue_comment,27989
bronze.events,events_watch,27097
bronze.events,events_issues,11786
bronze.events,events_fork,7062
bronze.events,other 9 event types,116480
events_push,gold.repo_daily_activity,162719
events_pull_request,gold.oss_health_mart,12000
events_issues,gold.oss_health_mart,9000
events_issue_comment,gold.oss_health_mart,9107
events_push,gold.bot_vs_human_activity_mart,162000
events_pull_request,gold.bot_vs_human_activity_mart,18000
events_issues,gold.bot_vs_human_activity_mart,2500
events_issue_comment,gold.bot_vs_human_activity_mart,8000
events_watch,gold.bot_vs_human_activity_mart,5000
events_fork,gold.bot_vs_human_activity_mart,3916
```

> [!NOTE]
> Gold-side weights are approximate proportions of the underlying
> Silver rows that contribute to each mart's aggregation (one row per repo-day
> ≠ one row per event). The point is the visual story of relative volume,
> not exact mart cardinality.

---

## Findings from the data

### Bot vs human activity, per repo-day

Across 199,416 `(repo, day)` rows in `bot_vs_human_activity_mart`, the
distribution is bimodal — most repo-days are entirely human or entirely bot,
with only a small "interesting tail" of mixed activity.

```mermaid
pie showData
    title Bot share across 199,416 repo-days
    "Pure human (0 % bot)" : 65.6
    "Pure bot (100 % bot)" : 32.0
    "Mixed activity" : 2.4
```

> [!IMPORTANT]
> Roughly **one third of repo-days are 100 % automated traffic**.
> This is the headline OSS-health finding — and exactly what made the
> Sprint 2.5 spike flag that the original bot rule needed a curated allowlist
> ([Rule C in ADR-0006](docs/adr/0006-bot-identification.md)), not just the
> `[bot]` suffix.

### Top bots by event count

`github-actions[bot]` dominates the bot signal in the sample. `LombiqBot`
is the visible miss for `[bot]`-suffix detection; the
[`known_bots.csv` allowlist](dbt/seeds/known_bots.csv) catches it via Rule C.

```text
github-actions[bot]       ████████████████████████████████████████  129,533
renovate[bot]             ██                                          6,480
dependabot[bot]           ██                                          6,203
pull[bot]                 █▍                                          4,491
swa-runner-app[bot]       █                                           3,635
LombiqBot ◄ Rule C        ▌  no [bot] suffix; allowlist catches it    1,752
sonarqubecloud[bot]       ▍                                           1,274
coderabbitai[bot]         ▍                                           1,108
```

### Top 10 multi-author repos on 2025-01-15

Filtered to `unique_pushers ≥ 5` to surface real OSS projects vs
single-actor automation.

| repo_name              | push_count | total_commits | unique_pushers | bot_push_count |
| ---------------------- | ---------: | ------------: | -------------: | -------------: |
| `NexusAILab/cdn`       |        131 |           131 |              5 |              0 |
| `odoo-dev/odoo`        |         75 |         2,428 |             49 |              0 |
| `LucasOtw/SAE3_Sco…`   |         69 |            84 |              7 |              0 |
| `hmcts/cnp-flux-co…`   |         38 |            42 |              5 |              0 |
| `demisto/content`      |         36 |           394 |             15 |              6 |
| `ZrenKix/PROJ2024`     |         35 |           236 |              6 |              0 |
| `grafana/grafana`      |         33 |           593 |             20 |              2 |
| `Matteo-K/PACT`        |         32 |            50 |              5 |              0 |
| `deckhouse/deckhouse`  |         30 |            34 |             13 |              0 |
| `MarcusZ98/Racketeers` |         29 |           159 |              7 |              0 |

---

## Batch ↔ streaming reconciliation (Sprint 6 MVP)

Replay one ingest hour of PushEvents to Redpanda → drain via Spark
Structured Streaming + Delta MERGE into a parallel `silver_streaming` table
→ compare row-by-row to the batch Silver table for the same hour.

```mermaid
sequenceDiagram
    participant Raw as GH Archive .json.gz
    participant Kafka as Redpanda topic<br/>(gh-events)
    participant Spark as Structured Streaming<br/>foreachBatch + MERGE
    participant SS as silver_streaming.events_push
    participant SB as silver.events_push (batch)
    participant R as reconcile.py

    Raw->>Kafka: replay.py — 181,221 messages,<br/>keyed by repo_id
    Kafka->>Spark: subscribe gh-events
    Spark->>SS: Delta MERGE on id<br/>(exactly-once via idempotency)
    R->>SB: row count, ∑ commit_size, id set
    R->>SS: row count, ∑ commit_size, id set
    Note over R,SS: Δ rows = 0 · Δ commits = 0<br/>orphan ids = 0 / 0
```

| Metric                |   Batch |   Streaming |   Δ |
| --------------------- | ------: | ----------: | --: |
| rows (`events_push`)  | **181,221** | **181,221** | **+0** |
| `commit_size` Σ       |     576,167 |     576,167 |  +0 |
| ids only in batch     |           — |           — |  0  |
| ids only in streaming |           — |           — |  0  |

> [!TIP]
> Exactly-once via `foreachBatch` + Delta MERGE on `id` — no
> separate offset store needed. The natural idempotency of MERGE
> ([ADR-0002](docs/adr/0002-event-id-idempotency.md)) carries through to the
> streaming side without additional moving parts.

<details>
<summary><b>Reproduce in 4 commands</b></summary>

```bash
docker-compose -f streaming/docker-compose.yml up -d
uv run python -m streaming.replay    --source data/raw/2025-01-15-12.json.gz
uv run python -m streaming.consumer
uv run python -m streaming.reconcile --ingest-hour 2025-01-15-12
```
</details>

---

## The deliberate incident drill

Sprint 5b: synthesize 200 PushEvent rows where `payload.size` was renamed to
`payload.commit_count`, ingest as a new `ingest_hour`, observe which gate
catches it — and what's silently poisoned in between.

```mermaid
flowchart LR
    A["bronze_ingest<br/>raw JSON STRING<br/>(ADR-0001)"]:::pass
    B["gate_bronze<br/>4 DQ checks"]:::pass
    C["silver build<br/>events_push"]:::pass
    D["gate_silver<br/>row-count match"]:::pass
    E["dbt test<br/>not_null commit_size<br/>200 NULL rows"]:::fail

    A -->|"PASS"| B
    B -->|"PASS"| C
    C -->|"PASS"| D
    D -->|"PASS"| E

    classDef pass fill:#d1fae5,stroke:#059669,color:#064e3b
    classDef fail fill:#fee2e2,stroke:#dc2626,color:#7f1d1d,font-weight:bold
```

| Step                 | Why it didn't catch (or did)                                                                                                                                                                                |
| -------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `bronze_ingest` PASS | Bronze stores `payload` as raw JSON STRING by design ([ADR-0001](docs/adr/0001-payload-handling.md)). The rename is invisible at this layer.                                                                  |
| `gate_bronze` PASS   | 4 checks (id-unique, type-in-set, public, created_at-not-null) all hold; payload contents aren't checked at Bronze.                                                                                          |
| `silver build` PASS  | `get_json_object(payload_raw, '$.size')` silently returns NULL on the 200 rows. SELECT projects NULL successfully.                                                                                            |
| `gate_silver` PASS   | Silver row count still equals Bronze filtered by type — NULLs are still rows. **This is the gate the postmortem added a regression check to.**                                                                |
| `dbt test` **FAIL**  | `not_null_events_push_commit_size`: Got 200 results, configured to fail if != 0.                                                                                                                              |

> [!CAUTION]
> **Root cause: gate-placement, not a missing test.** The dbt schema test
> catches it, but it runs at the end of the pipeline — Gold marts would
> already have consumed the bad Silver data. The fix is a one-line
> `coalesce(size, commit_count)` in `events_push.sql` plus a new
> `silver_commit_size_not_null` gate in
> [`quality/checks.py`](quality/checks.py) that moves detection between
> Silver and Gold, where it belongs.
>
> Full 5 Whys + lessons in
> [docs/postmortems/0001-schema-drift.md](docs/postmortems/0001-schema-drift.md).

---

## Performance — and the honest negative result

Hypothesis: `OPTIMIZE ... ZORDER BY (type)` on Bronze would prune per-type
filter reads in every Silver build. **Result: at this 4-partition scale, it
didn't.** Recording the failure honestly is the experiment's value.

### Per-type filter wall-clock

```text
events_push BEFORE         ████████████████████████████  2.73 s   files_read = 4
events_push AFTER          ████████                      0.89 s   files_read = 4   ◄ same prune, "speedup" is JIT warmup

full silver build BEFORE   ████████████████████████████  37.36 s
full silver build AFTER    ████████████████████████████▍ 38.34 s  ◄ no net change
```

### Bronze storage — the 2× temporary spike

```text
before OPTIMIZE                ████████████████  465 MB · 4 files
after OPTIMIZE (pre-VACUUM)    ████████████████████████████████  931 MB · 8 files  ⚠ 2×
after VACUUM (dev only, 0h)    ████████████████  466 MB · 4 files
```

> [!WARNING]
> **Why it was a no-op:** ZORDER re-arranges rows *within* a file but
> can't split below the file boundary. At 4 files, the smallest skip-unit is
> 25 % of the table — no prune possible. At 100× scale, worth re-running.
> The 2× storage spike is the empirical reason ADR-0009 (compact-daily,
> vacuum-weekly with 168h retention) is **mandatory, not aspirational**.
>
> Full 5-dimension report:
> [docs/performance/sprint5b_tuning.md](docs/performance/sprint5b_tuning.md).

---

## The seven senior signals

```mermaid
mindmap
  root((Seven<br/>senior signals))
    Idempotency
      MERGE on event_id
      Cross-layer verifiers in spark/jobs
    Backfill / replay
      Airflow params.start_hour
      runbooks/backfill.md
    Schema-drift tolerance
      ADR-0001 payload as raw JSON
      Postmortem 0001 (real injection)
    Data-quality gates
      quality/checks.py
      18 checks across 4 suites
      Each suite exits non-zero
    Perf tuning report
      sprint5b_tuning.md
      5 dimensions before / after
      Honest negative result
    Batch + streaming
      Redpanda + foreachBatch + MERGE
      181,221 events
      0 row reconcile delta
    Operational docs
      7 ADRs
      5 runbooks
      1 postmortem
```

| # | Signal | Lives in |
|---|--------|----------|
| 1 | **Idempotency** | Bronze + Silver + Gold all MERGE on natural ids; runtime invariants in [`spark/jobs/gold_verify.py`](spark/jobs/gold_verify.py), [`gold_health_verify.py`](spark/jobs/gold_health_verify.py), [`gold_bot_verify.py`](spark/jobs/gold_bot_verify.py) |
| 2 | **Backfill / replay** | Airflow DAG `params.start_hour` / `end_hour`; [`docs/runbooks/backfill.md`](docs/runbooks/backfill.md) |
| 3 | **Schema-drift tolerance** | [ADR-0001](docs/adr/0001-payload-handling.md) (Bronze contract) + [postmortem 0001](docs/postmortems/0001-schema-drift.md) (proven under a real injected break) |
| 4 | **DQ gates** | [`quality/runner.py`](quality/runner.py) — 18 checks across 4 suites, each exits non-zero to gate the next Airflow task |
| 5 | **Perf tuning report** | 5-dimension before/after with an honest negative result in [`docs/performance/sprint5b_tuning.md`](docs/performance/sprint5b_tuning.md) |
| 6 | **Batch + streaming story** | [`streaming/`](streaming/) — 181,221 events reconciled with 0 row delta |
| 7 | **Operational docs** | 7 ADRs + 5 runbooks + 1 postmortem + 3 mart design docs + 1 spike report |

---

## ADR registry — placed by reversibility × scope

The most senior signal in this project isn't any single ADR; it's that
every decision in the upper-right quadrant (hard to change × cross-cutting)
has a written rationale you can challenge.

```mermaid
quadrantChart
    title ADR placement
    x-axis "Easy to revisit" --> "Hard to change later"
    y-axis "Narrow scope" --> "Cross-cutting"
    quadrant-1 "Most senior signal · ADR-mandatory"
    quadrant-2 "Cross-cutting · review at quarter"
    quadrant-3 "Annotate in code"
    quadrant-4 "Hard but contained"
    "0001 payload handling": [0.88, 0.86]
    "0002 event_id idempotency": [0.92, 0.78]
    "0003 partition by ingest_hour": [0.80, 0.68]
    "0004 no surrogate keys": [0.55, 0.60]
    "0005 silver build strategy": [0.40, 0.65]
    "0006 bot identification": [0.45, 0.52]
    "0007 storage overhead": [0.35, 0.45]
```

| # | Title | Status |
|---|-------|--------|
| 0001 | [Bronze payload as raw JSON STRING + bounded probe](docs/adr/0001-payload-handling.md) | ✅ Accepted |
| 0002 | [`event_id` as sole idempotency key](docs/adr/0002-event-id-idempotency.md) | ✅ Accepted |
| 0003 | [Partition Bronze by `ingest_hour`, ZORDER by `created_at`](docs/adr/0003-partition-by-ingest-hour.md) | ✅ Accepted |
| 0004 | [No surrogate keys; use GitHub source ids directly](docs/adr/0004-no-surrogate-keys.md) | ✅ Accepted |
| 0005 | [Silver build strategy (tiered, demand-driven) + dbt adapter swap plan](docs/adr/0005-silver-build-strategy.md) | ✅ Accepted |
| 0006 | [Bot rule: Rule A (`[bot]` suffix) + Rule C (allowlist) + event-level `is_app_event`](docs/adr/0006-bot-identification.md) | ✅ Accepted |
| 0007 | [Bronze storage overhead — 1.7× raw `.json.gz`, planning constants](docs/adr/0007-bronze-storage-overhead.md) | ✅ Accepted |
| 0008 | Streaming time semantics (event vs processing time) | ⏳ Sprint 7 (optional) |
| 0009 | OPTIMIZE / VACUUM cadence — preview in `sprint5b_tuning.md` | ⏳ Sprint 9 (optional) |

---

## Sprint timeline

Original 6-week estimate revised to a realistic 8–10 weeks. Streaming was
de-scoped from a 4-week build to a 1-week MVP that ships the talking point.

```mermaid
timeline
    title OSS Pulse — Sprint progression
    Sprint 0 : Schema discovery
             : ADR-0001 payload handling
    Sprint 1 : Bronze ingest · first Silver
             : ADR-0002 idempotency
             : ADR-0003 partitioning
    Sprint 2 + 2.5 : First Gold mart
                   : ADR-0004 no surrogate keys
                   : Bot heuristic spike
    Sprint 3 : 2 more marts (health, bot)
             : 5 more Silver tables
             : ADR-0005, 0006
    Sprint 4 : DQ gates · Airflow DAG
             : 4 runbooks
    Sprint 5b : GitHub Actions CI
              : Perf tuning report
              : ADR-0007
              : Postmortem 0001
    Sprint 5a : Terraform .tf for S3 · IAM
              : terraform plan validated
              : (apply step left for deployment)
    Sprint 6 : Streaming MVP
             : Redpanda + foreachBatch + MERGE
             : 0 row reconcile delta
    Backlog : Sprint 7-9
            : Streaming production-grade
            : ADR-0008 · 0009
```

---

## Layout

```text
spark/                  PySpark jobs
  jobs/                 bronze_ingest · bronze_inspect · gold_verify ·
                        gold_health_verify · gold_bot_verify ·
                        bot_heuristic_spike · perf_bench · perf_vacuum ·
                        incident_inject · s3_smoke_test
  schemas.py            Bronze envelope schema (single source of truth)
  tests/                chispa-style pytest unit tests
quality/                lightweight DQ-gate framework (intentionally not full GE)
  checks.py             per-check functions returning CheckResult
  runner.py             per-layer suite CLI (exits non-zero on fail)
dbt/                    dbt-spark project
  models/silver/        6 event-type tables + schema yml
  models/gold/          3 marts + schema yml
  macros/               register_external_sources · delta_source ·
                        generate_schema_name · is_bot
  seeds/                known_bots.csv (Rule C allowlist, ADR-0006)
airflow/dags/           parameterized end-to-end DAG (oss_pulse_pipeline.py)
streaming/              Sprint 6 MVP (Redpanda + Structured Streaming)
  docker-compose.yml · replay.py · consumer.py · reconcile.py
terraform/              Sprint 5a IaC (S3 + IAM + KMS — .tf authored, plan validated, apply not performed)
data/                   (gitignored)
  raw/                  GH Archive .json.gz inputs
  bronze/               Delta-backed Bronze table
  streaming/            Sprint 6 silver_streaming table
docs/
  PROJECT_PLAN.md       canonical master plan
  index.html            visual showcase (GitHub Pages from /docs)
  img/banner.svg        hero banner used by this README
  adr/                  ADR-0001 .. 0007
  marts/                per-mart design docs
  runbooks/             backfill · schema_change · data_missing ·
                        airflow_setup · cloud_migration · cloud_apply_walkthrough
  postmortems/          0001-schema-drift
  performance/          sprint5b_tuning.md + raw bench JSON
  spikes/               time-boxed validation experiments
  video_demo_script.md  5-min recording script for the project demo
.github/workflows/      ci.yml (ruff · pytest · dbt parse + compile)
```

---

## Run it locally

> [!IMPORTANT]
> Requires **JDK 17**. Spark 3.5 / Hadoop 3.3.4 still call
> `Subject.getSubject`, which was removed in Java 18+. Tested on Amazon
> Corretto 17.

```bash
# Setup
git clone https://github.com/MistFall-Wang/oss-pulse && cd oss-pulse
uv sync
export JAVA_HOME=/Library/Java/JavaVirtualMachines/amazon-corretto-17.jdk/Contents/Home
export PATH="$JAVA_HOME/bin:$PATH"
export PYSPARK_SUBMIT_ARGS="--driver-memory 4g pyspark-shell"

# End-to-end on one ingest hour
uv run python -m spark.jobs.bronze_ingest --source data/raw/2025-01-15-12.json.gz --bronze-path data/bronze/events
uv run python -m quality.runner --layer bronze
cd dbt && uv run dbt deps && uv run dbt run --select silver && cd ..
uv run python -m quality.runner --layer silver
cd dbt && uv run dbt run --select gold && cd ..
uv run python -m quality.runner --layer gold
uv run python -m quality.runner --layer cross_mart
cd dbt && uv run dbt test

# Cross-layer verifiers (ground truth on the busiest rows)
uv run python -m spark.jobs.gold_verify
uv run python -m spark.jobs.gold_health_verify
uv run python -m spark.jobs.gold_bot_verify

# Streaming MVP
docker-compose -f streaming/docker-compose.yml up -d
uv run python -m streaming.replay --source data/raw/2025-01-15-12.json.gz
uv run python -m streaming.consumer
uv run python -m streaming.reconcile --ingest-hour 2025-01-15-12

# Deliberate incident drill (Sprint 5b)
uv run python -m spark.jobs.incident_inject
# ... observe each gate ...
uv run python -m spark.jobs.incident_inject --cleanup
```

---

<sub>Built by <b>Peter Wang</b> as a portfolio for Canadian Senior Data Engineer roles. The full master plan lives in <a href="docs/PROJECT_PLAN.md"><code>docs/PROJECT_PLAN.md</code></a>. For the interactive visual showcase: <a href="https://mistfall-wang.github.io/oss-pulse/"><b>mistfall-wang.github.io/oss-pulse</b></a>.</sub>
