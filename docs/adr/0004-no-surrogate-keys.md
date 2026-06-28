# ADR-0004: No surrogate keys; use GitHub source ids directly

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: Peter Wang
- **Tags**: keys, modeling, gold, silver
- **Codified from**: `dbt/models/gold/repo_daily_activity.sql` and
  `dbt/models/silver/events_push.sql` (Sprint 2 implementation)

## Context

Kimball-style warehouses default to surrogate keys for every dimension:
hash a natural key into an INT, join on the surrogate, sidestep
natural-key churn. Most senior data-modeling textbooks treat this as
near-mandatory.

OSS Pulse rejects this default. Across Silver and the first Gold mart,
the project uses GitHub-issued ids verbatim:

- `silver.events_push.id` — GitHub event id (STRING), already proven
  globally unique in ADR-0002 (0 dupes in 613,876 events across three
  sample years).
- `silver.events_push.repo_id`, `silver.events_push.actor_id`,
  `silver.events_push.org_id` — GitHub numeric ids, BIGINT.
- `gold.repo_daily_activity` keys on `(repo_id, activity_date)` — a
  composite natural key, no surrogate.

## Decision

For this project, **the GitHub source id IS the warehouse key**. No
surrogate keys are introduced at any layer.

Specifically:

| Layer | Key | Rationale |
|-------|-----|-----------|
| Bronze | `id` (event STRING) | Idempotency contract, ADR-0002 |
| Silver per-event-type tables | same `id`, MERGE'd | Preserves Bronze's contract through Silver |
| Silver dimensional columns | `repo_id`, `actor_id`, `org_id` direct | Joinable across Silver tables without lookup |
| Gold marts | composite of GitHub ids + business dimensions (e.g. `(repo_id, activity_date)`) | Composite is part of the grain — adding a surrogate would not make joins easier, only longer |

No `dim_repo` / `dim_actor` / `dim_org` tables are built in Sprint 2.
They will be considered in Sprint 3 only if a Gold mart needs repo or
actor *attributes* (description, owner type, account creation date)
that aren't already on the fact tables. Even then, the dimension's
primary key will be `repo_id` / `actor_id` — not a surrogate.

## Why the textbook default doesn't apply here

Surrogate keys exist to insulate the warehouse from problems this
project doesn't have:

1. **Natural-key drift** — natural keys that aren't truly stable
   (e.g. SKU codes that get renamed, customer emails that change).
   GitHub `id` is immutable. GitHub `login` and `repo_name` *do*
   drift, but they're never used as join keys — only as denormalized
   labels on facts.
2. **Cross-source key conflicts** — same surrogate-able entity
   represented differently in CRM vs ERP. OSS Pulse is single-source
   (GH Archive). There is nothing to reconcile.
3. **Type narrowing** — STRING natural keys joined many-to-many can
   be slow; INT surrogates are faster. Bronze event id is STRING but
   it joins one-to-one (each Silver row has exactly one Bronze
   ancestor). The other ids (`repo_id`, `actor_id`) are already
   BIGINT.
4. **SCD-style history tracking** — surrogate keys let a Type-2
   dimension carry many rows for the same business entity.
   `repo_daily_activity` doesn't track repo attribute history; if
   Sprint 3+ needs SCD for `dim_repo`, ADR-0004 will be revisited.

None of these forces apply at the project's current scope. Adding
surrogate keys would be cargo-culting the textbook past its actual
purpose.

## Consequences

**Positive**:
- Lineage from Gold metric back to GH Archive raw line is one column,
  not a join chain.
- A SQL query like
  `select repo_name from gold.repo_daily_activity where repo_id = 41881900`
  works in any tool, no lookup table needed.
- Eliminates the perennial bug where the surrogate generator drifts
  out of sync with upstream (e.g. duplicate hash collisions, restart
  resets a sequence).
- One fewer thing to test, document, or explain at interview.

**Negative**:
- The day a Gold mart needs to track repo renames *as history* (not
  as the latest label), this ADR must be revisited and SCD-2 added
  to `dim_repo`. Sprint 3 will know if that's needed.
- If the project later adds GitLab data, every cross-source join will
  need a source-prefixed key (`'gh:41881900'` or similar). At that
  point the project either splits into two warehouses or introduces
  surrogates retroactively. Sprint 10+ scope, not now.
- A new contributor used to surrogate-everywhere shops may ask why.
  This ADR is the answer.

## Status conditions for revisit

Re-open this ADR (Status → Superseded) when **any one** of:

1. A second data source is integrated and entity reconciliation across
   sources is needed.
2. A Gold mart requires repo-attribute history (SCD-2).
3. A GitHub id type or generation scheme changes upstream (extremely
   unlikely but documented for completeness).

## References

- ADR-0002 (event_id as idempotency key) — established id stability
- `docs/marts/gold_repo_daily_activity.md` — uses
  `(repo_id, activity_date)` composite explicitly
- `spark/jobs/gold_verify.py` — invariant test rests on `repo_id`
  being the same in Silver and Gold
