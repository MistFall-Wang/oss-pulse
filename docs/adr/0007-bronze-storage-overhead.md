# ADR-0007: Bronze storage overhead — Delta vs raw `.json.gz`

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: Peter Wang
- **Tags**: storage, bronze, cost, capacity-planning
- **Codified from**: Sprint 5b measurements
  (see [`docs/performance/sprint5b_tuning.md`](../performance/sprint5b_tuning.md))

## Context

The Bronze layer stores GH Archive events in Delta. The source files
are gzipped JSON. Before committing to cloud storage costs in Sprint
5a (S3) and before sizing capacity for a Sprint 4+ full-week backfill,
we need actual numbers on the storage overhead of Delta + gzipped
Parquet vs raw `.json.gz`. The Sprint 0 plan deferred this to "real
data" — now we have real data.

## Measurements

Bronze table at the end of Sprint 3 (4 ingest hours, 613,876 events):

| Layer | Bytes | File count | Notes |
|-------|-------|-----------:|-------|
| Raw `.json.gz` (4 files) | **267 MB** | 4 | uncompressed JSON would be ~3.6 GB based on gzip's typical 8-15x ratio for verbose JSON |
| Bronze Delta data | **453 MB** | 4 parquet files | data partitioned by `ingest_hour`, snappy-compressed |
| Bronze Delta `_delta_log` | **60 KB** | 7 JSON commit files | grows linearly with ingest events |
| Bronze Delta **total** | **453 MB** | — | _delta_log is rounding error |

| Breakdown | Bytes |
|-----------|------:|
| 2015-01-15-12 raw | 7.6 MB |
| 2018-01-15-12 raw | 23 MB |
| 2025-01-15-12 raw | 115 MB |
| 2025-01-15-13 raw | 123 MB |
| **Sum raw** | **267 MB** |
| **Bronze Delta** | **453 MB** |
| **Overhead ratio** | **1.70×** |

## Decision

**Bronze on Delta costs ≈ 1.7× the raw `.json.gz` footprint.** This
is the planning constant for capacity and S3 cost forecasts going
forward. Concretely:

- A full 24-hour day (24 ingest hours × ~120 MB/hour for 2025-era
  GH Archive) = ~2.9 GB raw → ~5 GB Bronze Delta
- A full 7-day backfill = ~21 GB raw → ~35 GB Bronze Delta
- A full year backfill = ~1.1 TB raw → ~1.9 TB Bronze Delta

S3 standard at $0.023/GB-month: a year of Bronze ≈ **$44/month**.
Negligible at this scale; meaningful at 10x once we add Silver+Gold +
streaming side-table (Sprint 6).

## Where the 70% overhead goes

1. **`payload_raw` is stored uncompressed within the parquet column.**
   Parquet's snappy compression on a JSON STRING is much weaker than
   gzip's on a JSON stream. Storing the JSON as a typed STRUCT would
   compress better but break ADR-0001's schema-drift containment.
   Worth re-evaluating in Sprint 6+ if storage cost dominates.
2. **Per-row metadata.** Each parquet file repeats column names, type
   metadata, page indexes, statistics. For our schema (15 columns,
   short rows), this overhead is a few percent.
3. **Delta `_delta_log` is negligible** at this scale (60 KB on 453 MB
   of data) but grows linearly with the number of commit operations.
   If we move to a streaming ingestion writing every few seconds, the
   log size becomes a real factor and needs periodic CHECKPOINT.
4. **OPTIMIZE doubles storage temporarily** until VACUUM runs. See
   Sprint 5b tuning report — pre-VACUUM Bronze was 931 MB (2.0×) for
   the same data.

## What this ADR does NOT say

- It does not say "Delta is wasteful." 1.7× is the price paid for
  ACID writes, time travel, schema enforcement, and Z-ORDER. The 1.7×
  is empirically the right magnitude for an OLAP-grade table.
- It does not preclude per-event-type pre-partitioning at scale.
  If Bronze grows past ~10 ingest_hour partitions and the per-type
  filter pattern in Silver dominates cost, partitioning Bronze
  additionally by `type` (or moving to Liquid Clustering if the
  runtime supports it) becomes worth the complexity. Sprint 9+.

## Status conditions for revisit

Re-open this ADR when **any one** of:

1. Bronze grows past ~100 GB and overhead absolute cost becomes
   non-trivial (>$2/month).
2. We pre-partition Bronze by something other than `ingest_hour`.
3. We switch table format (Iceberg / Hudi / Unity Catalog managed).

## References

- [Sprint 5b tuning report](../performance/sprint5b_tuning.md)
- ADR-0001 (payload as raw JSON STRING) — the cost we pay for
  schema-drift tolerance
- ADR-0003 (partition by ingest_hour) — the partition that determines
  the file-count math here
- ADR-0009 (OPTIMIZE/VACUUM cadence, Sprint 9) — codifies the
  retention window that bounds the post-OPTIMIZE 2× storage spike
