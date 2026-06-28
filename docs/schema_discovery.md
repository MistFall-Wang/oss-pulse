# Schema Discovery - GH Archive

**Date**: 2026-06-28
**Files inspected**: 2025-01-15 hours 12, 13 UTC (2 hourly files, 238 MB gzipped)
**Total events**: 529,351

This report turns a two-hour GH Archive sample into the first set of Bronze-layer
schema decisions. Raw evidence is preserved in
`docs/schema_discovery_raw_output.txt`.

## 1. Event type landscape

Top 10 event types account for 98.68% of all events:

- PushEvent: 64.91%
- CreateEvent: 10.82%
- PullRequestEvent: 6.37%
- IssueCommentEvent: 3.98%
- WatchEvent: 3.51%
- DeleteEvent: 2.63%
- PullRequestReviewEvent: 2.45%
- PullRequestReviewCommentEvent: 1.58%
- IssuesEvent: 1.57%
- ForkEvent: 0.86%

The sample contains 15 distinct event types. PushEvent dominates the workload,
while the bottom five types together account for only 1.32%.

**Decision implication**: Bronze must ingest every event type without branching,
because even long-tail events still need replay and auditability. Silver should
prioritize typed models for PushEvent, CreateEvent, PullRequestEvent,
IssueCommentEvent, and WatchEvent first, then add long-tail event models only
when product questions require them.

## 2. Top-level fields - what's universal, what's optional

| Field | Presence | Notes |
| --- | ---: | --- |
| id | 100.00% | Universal source event id. |
| type | 100.00% | Universal event discriminator. |
| actor | 100.00% | Universal nested actor object. |
| repo | 100.00% | Universal nested repository object. |
| payload | 100.00% | Universal nested event-specific object. |
| public | 100.00% | Universal boolean in this sample. |
| created_at | 100.00% | Universal UTC event timestamp string. |
| org | 26.40% | Sparse; present only for events tied to an organization. |

**Decision implication**: Bronze should use strong typed columns for stable
top-level facts: `id`, `type`, `actor_id`, `actor_login`, `repo_id`,
`repo_name`, `public`, `created_at_raw`, `created_at_ts`, `ingest_hour`, and
`source_file`. `org_id` and `org_login` should be nullable typed columns, not a
required nested object. `payload` should remain a raw JSON string in Bronze so
event-specific schema changes do not break ingestion.

## 3. event_id uniqueness

- 2-hour duplicate id count: **0** (0.00%)
- Cross-hour duplicate id count: **0**
- Typical duplicate scenario observed: none in this sample

**Decision implication**: Use `id` as the Bronze idempotency key. Do not include
`ingest_hour` in the dedupe key, because retrying or replaying an already
processed hourly file should collapse to the same source event. Keep
`ingest_hour` and `source_file` as lineage columns for partitioning, audits, and
backfills, but not as identity. The first implementation should use a Delta
`MERGE` on `target.id = source.id`; add duplicate-rate monitoring because this
sample is evidence, not a lifetime guarantee.

## 4. payload schema drift

Payload shape diverges sharply by event type. PushEvent carries commit and ref
fields, PullRequestEvent carries a deep `pull_request` object, IssueCommentEvent
carries both `issue` and `comment`, and WatchEvent only exposes `action` in the
top-level payload fields inspected here. Within the top five event types, the
listed first-level payload keys were present in 100% of events for that type,
but their nested contents and depths vary materially.

**Decision implication**: Choose Bronze as raw `payload` JSON string plus typed
top-level columns, then parse payloads in Silver by `type`. Do not let Spark
infer a single Bronze `STRUCT` for payload; one global struct would be wide,
sparse, and brittle under GH Archive schema evolution. Silver can use typed
tables for high-volume or high-value event types, starting with `silver_push`
and `silver_pull_request`. The trade-off is that some parsing moves later in the
pipeline, but ingestion remains replayable and tolerant of schema drift.

## 5. Nesting depth

Observed payload nesting depth among the top five event types:

| Event type | Max depth | Average depth |
| --- | ---: | ---: |
| PushEvent | 4 | 3.99 |
| CreateEvent | 1 | 1.00 |
| PullRequestEvent | 5 | 5.00 |
| IssueCommentEvent | 4 | 3.72 |
| WatchEvent | 1 | 1.00 |

**Decision implication**: Silver should not fully flatten every payload by
default. Flatten stable, frequently queried keys such as action, ref, PR number,
and push sizes. Keep deep or large objects such as commits, pull_request, issue,
and comment as structs or JSON columns until a concrete metric needs them. This
avoids creating a fragile, overly wide Silver schema on day one.

## 6. Timestamp & timezone

- Format: `2025-01-15T12:00:00Z`
- Samples are ISO 8601 UTC strings with a trailing `Z`.
- No timezone ambiguity was observed.

**Decision implication**: Bronze should keep both `created_at_raw` and parsed
`created_at_ts`. Partitioning should initially use the ingestion/source hour
derived from the file name, not only `created_at_ts`, because replay and hourly
file lineage are operational concerns. Event-time partitioning can be evaluated
in ADR-003 once query patterns are known.

## 7. ID types

- `actor.id` is INTEGER.
- `repo.id` is INTEGER.
- `org.id` is INTEGER when present.
- `actor.login`, `repo.name`, and `org.login` are STRING.

**Decision implication**: Dimension tables should use GitHub source ids as their
natural primary keys for the first implementation: `actor_id`, `repo_id`, and
`org_id`. Do not introduce warehouse surrogate keys yet. Surrogates add joins
and state management before there is a demonstrated need; source ids are stable,
portable across replays, and easy to reconcile with raw events.

## Open questions

- [ ] Should sparse `org` become a separate `dim_org`, or remain nullable on
      event rows until org-level questions appear?
- [ ] Are payload-level users always equivalent to top-level `actor`, or can
      they represent different participants?
- [ ] What is the relationship between `payload.pull_request.user` and the
      top-level `actor` for opened, closed, reviewed, and synchronized PRs?
- [ ] Which Silver event types are worth typed tables in Sprint 1 beyond
      PushEvent and PullRequestEvent?
