# ADR-0001: Bronze payload handling - raw JSON string plus bounded probe struct

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: Peter Wang
- **Tags**: schema-drift, bronze, ingestion

## Context

GH Archive emits ~15 distinct event types as of 2025, each with its own
`payload` schema. Empirical inspection across three sample years
(see `docs/schema_drift_evidence.md`) reveals:

| Event type        | 2015 nested paths | 2025 nested paths | Drift |
|-------------------|-------------------|-------------------|-------|
| PushEvent         | 14                | 15                | +1    |
| IssueCommentEvent | 120               | **309**           | +189  |

Surface-level field stability is misleading: `IssueCommentEvent` kept
identical top-level payload keys (`action`, `comment`, `issue`) across a
decade, but the `comment` and `issue` subtrees grew by 189 nested paths,
largely from GitHub Apps integration (`performed_via_github_app.*`).

At the same time, the top-level event envelope stayed stable across the
2015, 2018, and 2025 samples: `actor`, `created_at`, `id`, `org`,
`payload`, `public`, `repo`, and `type` were observed with the same value
types. This means the envelope can be strongly typed, while nested
payloads need a drift-tolerant landing strategy.

A new event type (`PullRequestReviewEvent`) appeared in the 2025 sample
that does not exist in the 2015/2018 samples, confirming that the type set
is also not stable across years.

## Decision

The Bronze layer stores `payload` **as raw JSON STRING** in column
`payload_raw`. This column is the source of truth for all nested payload
data.

Bronze may also store a secondary `payload_probe` STRUCT with a small,
bounded set of nullable first-level fields used for ingestion QA and
debugging. Initial candidates are `action`, `push_id`, `repository_id`,
`size`, and `distinct_size`. `payload_probe` must never become a full
typed representation of the payload and must not include deep nested
objects.

Silver layer parses `payload_raw` per event type into strongly-typed
tables (`silver.events_push`, `silver.events_pull_request`, ...) on
demand, driven by Gold mart requirements (see ADR-0005 for Silver
build strategy).

## Probe field selection criteria

A field qualifies for `payload_probe` if and only if it satisfies all of:

1. Top-level under `payload` (no nested struct extraction)
2. Used by ingest-time data quality expectations or operational debugging
3. Either universally present in at least one common event type, or
   intentionally tracked as a drift indicator

Initial probe fields:

| Field           | Source event types       | Used by                              |
|-----------------|--------------------------|--------------------------------------|
| `action`        | PullRequest, Issues, ... | Ingest QA: action enumeration check  |
| `push_id`       | PushEvent                | Ingest QA: PushEvent non-null check  |
| `repository_id` | PushEvent (2017+)        | Cross-year schema drift indicator    |
| `size`          | PushEvent                | Ingest QA: numeric range check       |
| `distinct_size` | PushEvent                | Ingest QA: numeric range check       |

Adding a new probe field requires a PR that updates this table and
references which selection criterion is satisfied.

## Consequences

**Positive**

- Bronze ingestion never fails on schema drift; any JSON document is
  valid input
- Silver parsing logic can be fixed or extended without re-ingesting
  Bronze; rerun from `payload_raw`
- Cross-year backfill (e.g. 2015-2025) writes to a single Bronze table
  without schema conflicts
- New event types appear in Bronze automatically; only Silver needs
  intentional extension
- Top-level envelope columns remain strongly typed, preserving cheap
  filtering and lineage without over-typing nested payloads

**Negative**

- Storage cost increases from storing both `payload_raw` and
  `payload_probe`. The exact overhead is unknown until the first Bronze
  implementation writes Delta files; measure it in Sprint 1 before using
  a numeric claim in performance documentation.
- Bronze querying `SELECT payload_raw` returns raw JSON; engineers must
  use Silver tables or `from_json` for typed access. This is acceptable
  because Bronze is not an analyst-facing layer.
- `payload_probe` can become scope creep if treated as a convenience
  schema. Any added probe field must support ingestion QA or debugging,
  not hypothetical downstream analytics.

**Neutral**

- Establishes the precedent that *every* nested schema-volatile source
  in this project lands as raw text in Bronze.

## Alternatives considered

### A. Bronze with full STRUCT, schema inferred at write time

Rejected. Spark `inferSchema` on JSON yields per-batch schemas. Two
hourly files in the same day can produce divergent struct columns
(e.g. one batch missing all `performed_via_github_app` fields), forcing
constant `mergeSchema = true` writes and producing a Bronze table with
hundreds of mostly-null columns. Cross-year backfill compounds the
problem.

### B. Per-type Bronze tables (`bronze.events_push`, `bronze.events_pull_request`, ...)

Rejected. Pushes the schema-drift problem from Bronze into ingestion
routing logic, requires DAG-level branching on event type, and creates
N tables with N maintenance burdens. A new event type appearing
upstream can be dropped or misclassified if there is no explicit catch-all
route. The whole point of Bronze is to be the durable, schema-agnostic
landing zone; splitting it defeats that.

### C. Store `payload_raw` only, with no probe struct

Rejected, but only narrowly. Pure raw-JSON Bronze is the most
schema-tolerant choice, but every downstream query (including
ingest-time QA checks like "does a PushEvent have `push_id`?") would need
`from_json` parsing. A bounded `payload_probe` gives cheap operational
checks without committing Bronze to a full nested payload schema.

## Open questions

- What is the measured storage overhead of `payload_raw` plus
  `payload_probe` after Delta compression?
- Which exact fields belong in the initial `payload_probe` contract?
- Should Bronze access patterns and SLAs get their own ADR once analysts
  or downstream jobs start querying Bronze directly?

## References

- Evidence: `docs/schema_drift_evidence.md`, `docs/schema_drift_evidence_raw.txt`
- Related: ADR-0002 (event_id idempotency), ADR-0005 (Silver build strategy, planned)
