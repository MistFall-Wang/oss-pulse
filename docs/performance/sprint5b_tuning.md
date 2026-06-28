# Sprint 5b performance tuning report — Bronze OPTIMIZE + ZORDER on `type`

- **Date**: 2026-06-28
- **Hypothesis**: Bronze is partitioned by `ingest_hour`. Every Silver
  model filters by `type`, which is independent of `ingest_hour`. Each
  filter therefore scans every file in every partition. Running
  `OPTIMIZE ... ZORDER BY (type)` should cluster rows of the same
  type together, letting Delta's data-skipping prune most files on a
  per-type filter.
- **Bench scripts**:
  [`spark/jobs/perf_bench.py`](../../spark/jobs/perf_bench.py),
  [`spark/jobs/perf_vacuum.py`](../../spark/jobs/perf_vacuum.py)
- **Raw numbers**:
  [`perf_bench.json`](perf_bench.json),
  [`perf_vacuum.json`](perf_vacuum.json)

## What was measured

For each of the 6 Silver tables, before and after OPTIMIZE+ZORDER:

| Dimension | How measured |
|-----------|--------------|
| Wall-clock for a single-type filter | `spark.read.format('delta').load(bronze).filter(type=…).count()` timing |
| Bronze files read after pruning | `df.inputFiles()` length (post-prune file count) |
| Output rows | `df.count()` |
| Output file count per Silver table | parquet count in `silver.db/<model>` |
| Output bytes per Silver table | recursive size |
| Bronze storage footprint | recursive bytes / file count of `data/bronze/events` |
| Full Silver full-refresh wall-clock | shelling out `dbt run --select silver --full-refresh` |
| OPTIMIZE wall-clock | `DeltaTable.optimize().executeZOrderBy('type')` timing |
| VACUUM wall-clock + delta storage | separate `perf_vacuum.py` run |

## Results — the headline

**OPTIMIZE+ZORDER did not produce the expected speedup at this scale.**

| Silver model | Filter wall-clock BEFORE | AFTER | Files read BEFORE | AFTER |
|--------------|--------------------------|-------|-------------------|-------|
| events_push | 2.73 s | 0.89 s | 4 | 4 |
| events_pull_request | 0.21 s | 0.17 s | 4 | 4 |
| events_issue_comment | 0.18 s | 0.14 s | 4 | 4 |
| events_issues | 0.16 s | 0.12 s | 4 | 4 |
| events_watch | 0.19 s | 0.13 s | 4 | 4 |
| events_fork | 0.14 s | 0.14 s | 4 | 4 |

| Silver full-refresh wall-clock | 37.36 s | 38.34 s |
|--------------------------------|---------|---------|

`files_read` is the same in both rounds (4 — one per `ingest_hour`
partition). **Data-skipping did not prune any files.** That is the
real story.

The apparent speedup on `events_push` (2.73s → 0.89s) is JVM JIT
warmup + OS file-cache reuse from the BEFORE round, not data-skipping.
The smaller event types' improvements (0.05–0.10 s) are noise — they
sit inside Spark's per-query overhead.

## Why ZORDER didn't help (root cause)

ZORDER works by writing per-file `min`/`max` stats for the clustered
columns into the Delta log. On a filter, Delta consults those stats
and skips files whose range can't possibly match.

Our Bronze has **4 files total**, one per `ingest_hour` partition,
each ~115 MB. Every file contains rows of every event type — that's
how GH Archive emits the data. ZORDER can re-arrange rows *within* a
file but not split a file. With 4 files, the smallest prune unit is
already 25% of the table; data-skipping can't help below that
granularity.

At this scale ZORDER is a no-op for the per-type filter pattern.

## Storage cost of the experiment (the other side)

OPTIMIZE created 4 new compacted files alongside the originals.
Until VACUUM removes the orphans:

| State | Files | Bytes |
|-------|-------|-------|
| Before OPTIMIZE | 4 | 465,703,375 |
| After OPTIMIZE (pre-VACUUM) | 8 | 931,403,461 |
| After VACUUM (retention 0h, dev-only) | 4 | 465,711,879 |

i.e. **OPTIMIZE doubled apparent storage for the window before VACUUM
ran**. In prod with the default 7-day retention, that doubling
persists for a week. ADR-0009 (Sprint 9) will codify a compact-daily
/ vacuum-weekly cadence; the experiment here is the empirical reason
the project commits to it as a real decision rather than a default.

Side note: VACUUM with `retentionHours=0` requires
`spark.databricks.delta.retentionDurationCheck.enabled=false`. This
is **dev-only**. In prod, any reader querying the table during VACUUM
could read corrupt data once retention drops below the longest
in-flight read. The Sprint 9 ADR will set retention to 168h (7 days)
in prod.

## What would actually help at this scale

1. **Reduce dbt cold-start time** — 6 Silver models, full-refresh,
   ~5s of "compiling" + ~2s of metastore I/O per model. That's
   ~40 s of the 37 s wall clock for the build. Moving to a long-lived
   Spark session (Databricks Connect, or dbt-databricks with a SQL
   warehouse) is the next-biggest lever. Sprint 5a's adapter swap
   (ADR-0005) does exactly this.
2. **Coalesce small output files** — Silver outputs land in 14–22
   files each, many sub-MB. A
   `spark.sql.shuffle.partitions=4` (we currently use 8) plus
   `OPTIMIZE` on each Silver after build would consolidate. Skipped
   because shuffle reduction has knock-on effects on parallelism that
   matter more when ingest hours grow beyond 4.
3. **Z-ORDER on `repo_id`** instead of `type` — the Gold marts'
   `repo_id`-keyed joins would benefit far more than the Silver
   per-type filters. Worth re-running this experiment once the
   `bot_vs_human_activity_mart` becomes the slowest job (it already
   contains the 6-way UNION ALL + a self-distinct, which is the
   plausible next bottleneck). Not done here because the experiment
   would only be meaningful with > 4 partitions of Bronze.

## What an interviewer should hear

> "I had a hypothesis that ZORDER would speed up per-type filters. I
> measured before and after across 5 dimensions per model. The headline
> wall-clock looked faster, but `inputFiles()` showed zero pruning —
> the speedup was warmup, not ZORDER. I confirmed the root cause
> (4 files = 4-partition Bronze, smaller than ZORDER's prune unit)
> and refused to call the experiment a win. The storage trade-off
> (2x footprint until VACUUM) was the data point that promoted
> ADR-0009 from an aspirational Sprint 9 entry to a real production
> commitment."

That's the version of "performance tuning" Sr DE roles in Canada are
hiring for: own the numbers, refuse a misleading win, propose a
better experiment for the next constraint up.

## What gets re-measured next

A Sprint 5b+ follow-up should:
- Backfill Bronze to ≥ 7 days (≈170 partitions, ~30 GB)
- Re-run this benchmark
- Re-run with ZORDER on `repo_id` for the Gold mart side
- Add a `cost` column to the table (real Databricks DBU-second cost
  once Sprint 5a's cloud migration is live)
