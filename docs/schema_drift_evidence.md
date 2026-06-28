# Schema Drift Evidence - GH Archive

**Date**: 2026-06-28
**Script**: `notebooks/02_schema_drift_evidence.py`
**Raw output**: `docs/schema_drift_evidence_raw.txt`

This note summarizes the cross-year GH Archive samples used as evidence for
ADR-0001. It compares one UTC hour from 2015, 2018, and 2025.

## Samples

| Sample hour | Gzipped file size | Events | Distinct event types |
| --- | ---: | ---: | ---: |
| 2015-01-15 12:00 UTC | 7.6 MB | 21,062 | 14 |
| 2018-01-15 12:00 UTC | 23 MB | 63,463 | 14 |
| 2025-01-15 12:00 UTC | 115 MB | 270,553 | 15 |

The hourly event volume grew 12.8x from 2015 to 2025 in this sample set.

## Top-Level Envelope

The observed top-level field vocabulary is identical across all three samples:

| Field | 2015 type | 2018 type | 2025 type |
| --- | --- | --- | --- |
| actor | object | object | object |
| created_at | str | str | str |
| id | str | str | str |
| org | object | object | object |
| payload | object | object | object |
| public | bool | bool | bool |
| repo | object | object | object |
| type | str | str | str |

This supports strong typed Bronze columns for the stable event envelope. It does
not mean every field is present on every event: `org` remains sparse, as shown
in `docs/schema_discovery.md`.

Nested source ids are also stable integer values in all three samples:

| Entity | 2015 present / id type | 2018 present / id type | 2025 present / id type |
| --- | --- | --- | --- |
| actor | 21,062 / int | 63,463 / int | 270,553 / int |
| repo | 21,062 / int | 63,463 / int | 270,553 / int |
| org | 7,535 / int | 22,646 / int | 68,567 / int |

## Event Type Set

The 2015 and 2018 samples both contain 14 event types. The 2025 sample contains
15 event types and adds `PullRequestReviewEvent`.

**Implication**: Bronze ingestion should not depend on a closed event-type
whitelist. New upstream event types must still land durably in Bronze before any
Silver model exists for them.

## Payload Drift

Payload drift is not uniform across event types.

| Event type | 2015 nested paths | 2018 nested paths | 2025 nested paths | Drift from 2015 to 2025 |
| --- | ---: | ---: | ---: | ---: |
| PushEvent | 14 | 14 | 15 | +1 |
| IssueCommentEvent | 120 | 144 | 309 | +189 |

For `PushEvent`, 2025 adds one first-level payload field: `repository_id`.

For `IssueCommentEvent`, first-level payload keys stay exactly the same across
the decade: `action`, `comment`, and `issue`. The drift happens below that
stable surface. New nested paths include:

- `comment.author_association`
- `comment.node_id`
- `comment.performed_via_github_app`
- `comment.performed_via_github_app.client_id`
- `comment.performed_via_github_app.events`
- `comment.performed_via_github_app.owner.login`

**Implication**: a full inferred Bronze `STRUCT` for `payload` would look stable
at the first level while silently growing hundreds of nested fields. Cross-year
backfills would produce old rows with large null subtrees and future years would
force repeated schema evolution.

## Bot Activity Note

`comment.performed_via_github_app.*` appears in the 2025 nested path set and is
absent from the 2015 sample. This field family is useful for bot and app-driven
activity detection, but historical bot metrics must be era-aware: samples before
that field exists cannot be compared naively with samples after it appears.

This belongs in the future Bot Activity Mart design, not ADR-0001, but the
evidence should remain close to the architecture notes.

## Limits

These are three one-hour samples, not a full historical audit. The evidence is
strong enough to choose a schema-drift-tolerant Bronze strategy, but not enough
to claim exact introduction dates for specific nested fields.
