# ADR-0002: `event_id` as the sole idempotency key for Bronze

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: Peter Wang
- **Tags**: idempotency, bronze, ingestion
- **Codified from**: `spark/jobs/bronze_ingest.py` (commit <fill in hash>)

## Context

The Bronze ingestion job re-runs are operational reality:

- Airflow task retries on transient failures
- Manual re-runs of a known-bad hour
- Backfill ranges that overlap previously ingested data
- Re-processing after GH Archive republishes a file

Empirical observation across the discovery samples:

| Sample window           | Events  | Duplicate `id`s | Cross-hour `id` overlap |
|-------------------------|---------|-----------------|-------------------------|
| 2025-01-15, hours 12-13 | 529,351 | 0               | 0                       |
| 2015-01-15, hour 12     | 21,062  | 0               | n/a (single hour)       |
| 2018-01-15, hour 12     | 63,463  | 0               | n/a (single hour)       |
| **Combined Bronze**     | **613,876** | **0**       | **0**                   |

`event.id` is a GitHub-issued STRING that is globally unique across all
events ever emitted. GitHub guarantees id stability and non-reuse as
part of its public API contract.

## Decision

Bronze treats `event.id` (column `id STRING`) as the **sole primary
key** for deduplication.

Idempotency is enforced via Delta `MERGE INTO ... USING ... ON
target.id = source.id WHEN NOT MATCHED THEN INSERT ALL`. Events are
**append-only** at the Bronze level. There is no `UPDATE` branch; once
landed, an event row is immutable.

Operational metadata (`ingest_hour`, `ingest_run_id`, `source_file`) is
recorded as columns on the Bronze row at first insert, but is **not**
part of the primary key.

## Implementation

```python
# spark/jobs/bronze_ingest.py — write_bronze()
target.alias("t") \
    .merge(batch.alias("s"), "t.id = s.id") \
    .whenNotMatchedInsertAll() \
    .execute()
```

## Verified invariants

After every Bronze write, the following must hold:

1. `count(*) = count(distinct id)` on the Bronze table
2. Re-ingesting any file N times produces the same total row count

Both verified manually after Sprint 1 step 3:
- Single-file ingest: 270,553 events → 270,553 unique ids
- Cumulative 4-file ingest: 613,876 rows, 613,876 unique ids
- Same-file second run: total unchanged at 613,876

## Consequences

**Positive**

- Re-running any hourly Airflow task is safe by construction
- Backfill ranges may freely overlap; the `MERGE` collapses overlap to
  a single landed event
- Idempotency contract is verifiable by a single SQL invariant
- Survives schema drift documented in ADR-0001: deduplication does not
  touch payload contents

**Negative**

- Trusts GitHub's id uniqueness guarantee. Mitigation: planned Great
  Expectations check `expect_column_values_to_be_unique(id)` after
  every Bronze write (Sprint 4)
- `MERGE` is more expensive than blind `INSERT`. Accepted cost; the
  alternative (staging + dedup) moves cost to a second Spark stage
  with worse lineage

**Neutral**

- Establishes the precedent: operational metadata never participates
  in business primary keys across this project

## Alternatives considered

### A. Composite key `(id, ingest_hour)`

Rejected. Intent of re-ingesting an hour is to *collapse* to the same
logical event, not record N copies tagged by ingest attempt. If an
Airflow task retries in the next clock hour, `(id, ingest_hour)` would
treat the same event as two distinct rows. This breaks idempotency at
the row level and forces every downstream consumer to dedup on `id`
anyway — the composite key offers no benefit while multiplying
storage.

### B. Composite key `(id, type, created_at)`

Rejected. Each component is redundant given `id`: a GitHub event with
a fixed id has exactly one `type` and one `created_at`. The composite
key adds storage and MERGE cost with no semantic gain.

### C. Append-only with offline dedup

Rejected. "Insert everything, dedup later in Silver" defers the
idempotency contract to a downstream layer. Bronze then exposes
duplicate `id`s to any reader between ingest and dedup, violating the
principle that each medallion layer should be correct on its own
terms. MERGE cost saved at Bronze is paid back (with interest) in
Silver.

## Open questions

- Should the `MERGE` ever take an `UPDATE` branch? GH Archive
  occasionally republishes corrected files. If a republished event has
  the same `id` but different payload bytes, do we keep the original
  or overwrite?
  - **Resolved by**: Sprint 2 backfill testing
  - **Method**: deliberately re-ingest a known-republished hour and
    compare payload bytes; decide policy based on observed semantics

## References

- Evidence: `docs/schema_discovery_raw_output.txt`,
  `docs/schema_drift_evidence_raw.txt`
- Code: `spark/jobs/bronze_ingest.py` (write_bronze)
- Related: ADR-0001 (payload handling), ADR-0003 (partition strategy)