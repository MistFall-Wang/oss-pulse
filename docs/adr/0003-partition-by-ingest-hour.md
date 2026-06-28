# ADR-0003: Bronze partition by `ingest_hour`, ZORDER by `created_at`

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: Peter Wang
- **Tags**: storage-layout, bronze, performance
- **Codified from**: `spark/jobs/bronze_ingest.py` (commit <fill in hash>)

## Context

Bronze must support two query patterns:

1. **Operational**: "Re-ingest hour 2025-01-15-12" — bounded by the
   hourly file boundary
2. **Analytical**: "Events between 2025-01-15 12:00 and 14:00" —
   bounded by event business time

Data volume grows over time. Observed in samples:

| Sample year | Events per hour | Growth vs 2015 |
|-------------|-----------------|----------------|
| 2015        | 21,062          | 1.0x           |
| 2018        | 63,463          | 3.0x           |
| 2025        | 270,553         | 12.8x          |

A partitioning column tied to `created_at` would create wildly uneven
partitions across years. A partitioning column tied to ingest mechanics
(one partition per source file) gives stable, predictable file
boundaries regardless of data volume.

## Decision

**Partition column**: `ingest_hour STRING` (format `YYYY-MM-DD-HH`).

- One partition per hourly GH Archive file
- Partition value derived from the source filename at ingest time
- `ingest_hour` is intentionally STRING, not TIMESTAMP — it is a file
  identifier, not a business timestamp

**ZORDER columns** (planned, applied via `OPTIMIZE` in Sprint 5):
`(created_at, repo_id)`.

- `created_at` accelerates analytical queries that filter on business
  time
- `repo_id` accelerates the most common Gold mart access pattern
  (per-repo aggregations)

## Implementation

```python
# spark/jobs/bronze_ingest.py — write_bronze() (first-write branch)
batch.write.format("delta") \
    .partitionBy("ingest_hour") \
    .mode("overwrite") \
    .save(bronze_path)
```

## Consequences

**Positive**

- Re-ingesting one hour rewrites exactly one partition; rest of the
  table untouched (Delta partition-level transactional isolation)
- Backfill of arbitrary date ranges maps cleanly to a set of partitions
- File listing for a specific source file is O(1) directory lookup
- `count(*)` per hour is metadata-only via Delta partition stats

**Negative**

- Analytical queries on `created_at` cannot use partition pruning
  directly; rely on file-level data skipping plus ZORDER
- Partition count grows linearly with backfill range (24 partitions/day
  × N days). At 10 years of hourly backfill that's ~87,600 partitions.
  Acceptable for Delta but requires `OPTIMIZE` compaction to keep file
  counts sane (planned ADR-0008)

**Neutral**

- Because `ingest_hour` ≈ `created_at_hour` for fresh data (events
  enter GH Archive within ~minutes), partition pruning on `ingest_hour`
  effectively prunes by business time for most analytical queries too
- This near-equivalence is a coincidence of GH Archive's ingest
  cadence, not a contract. Backfills break the equivalence — that's
  fine, ZORDER handles it

## Alternatives considered

### A. Partition by `created_at_date` (one partition per day)

Rejected. Each day partition would contain events from 24 source
files. Re-ingesting one hour would require either:
- Reading 23 other hours' worth of data to rewrite the day partition
  (expensive), or
- Letting Delta append-with-merge handle it (loses the "one file =
  one partition" mental model)

The operational simplicity of "1 hour file ↔ 1 partition" is more
valuable than the analytical convenience of date-based pruning.

### B. Partition by `created_at_hour` (parsed from event business time)

Rejected. Equivalent to `ingest_hour` for fresh data, but for
backfills where ingest time ≠ event time, this creates partitions that
don't align with source files. Re-ingesting `2025-01-15-12.json.gz`
might write to partitions for any event timestamp inside that file —
including events with `created_at` from 2025-01-15-11 (events that
crossed the hour boundary at the API level).

GH Archive in practice keeps events tightly within their hour, but the
contract is "events ingested in this file", not "events created in
this hour". Partitioning should follow the contract, not the typical
behavior.

### C. No partitioning, ZORDER only

Rejected for Bronze. At backfill scale (years of data), no
partitioning means one giant Delta table. Single-hour re-ingest then
requires Delta to scan file-level stats across the whole table to find
overlapping files. Partitioning gives O(1) operational locality at
trivial cost.

## Open questions

- What is the actual performance delta of analytical queries on
  `created_at` with and without ZORDER, at our scale?
  - **Resolved by**: Sprint 5 performance tuning report
  - **Method**: same query, before/after `OPTIMIZE ... ZORDER BY
    (created_at, repo_id)`, capture five-dimension metrics (data
    volume, wall clock, shuffle write, file count, cost)

## References

- Code: `spark/jobs/bronze_ingest.py` (write_bronze, first-write branch)
- Related: ADR-0002 (idempotency), ADR-0008 (OPTIMIZE/VACUUM cadence,
  planned)