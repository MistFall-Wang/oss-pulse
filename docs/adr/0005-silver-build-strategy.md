# ADR-0005: Silver build strategy (tiered, demand-driven) + dbt adapter swap plan

- **Status**: Accepted
- **Date**: 2026-06-28
- **Deciders**: Peter Wang
- **Tags**: silver, modeling, dbt, adapter, build-order
- **Codified from**: Sprint 1 (`events_push`) + Sprint 3a
  (`events_pull_request`, `events_issues`, `events_issue_comment`) +
  Sprint 3b (`events_watch`, `events_fork`)

## Context

The Bronze layer stores all 15+ GH Archive event types in one Delta
table, with the per-type payload kept as raw JSON string (ADR-0001).
The Silver layer's job is to flatten that JSON per event type into
strongly-typed tables.

A naive read of "build Silver" is "write 15 dbt models, one per
event type". This is wrong for two reasons:

1. **Most event types don't yet feed a Gold mart.** Building
   `events_member`, `events_gollum`, `events_public` ahead of demand
   adds maintenance surface (schema yml, tests, schema drift watch)
   for tables nobody queries.
2. **Each Silver table is a contract** — once it exists, downstream
   may depend on it, and removing it later is breaking. Adding
   prematurely is therefore not free.

The same logic applies to the adapter choice. Locally the project
runs `dbt-spark` against an embedded session. On Databricks the
correct adapter is `dbt-databricks` (Unity Catalog support, SQL
warehouse integration). Discovering adapter incompatibilities the day
of the Sprint 5a cloud migration is the wrong time.

## Decision

### Build Silver tables on demand, not on inventory

A new `silver.events_<type>` model is created **only when a Gold mart
needs it**. The Silver layer grows tier-by-tier:

| Tier | Event types | Why now | Sprint |
|------|-------------|---------|--------|
| 1 | `PushEvent` | feeds `gold.repo_daily_activity` | Sprint 1 |
| 2 | `PullRequestEvent`, `IssuesEvent`, `IssueCommentEvent` | feed `gold.oss_health_mart` | Sprint 3a |
| 3 | `WatchEvent`, `ForkEvent` | feed `gold.bot_vs_human_activity_mart` (cross-event coverage) and the future Repo Growth mart | Sprint 3b |
| 4 | `PullRequestReviewEvent`, `PullRequestReviewCommentEvent` | not yet — would add review-cycle metrics if asked | not scheduled |
| 5 | other event types (Release, Member, Public, Gollum, CommitComment, Create, Delete, ...) | not yet — payload accessible from Bronze on demand | not scheduled |

Each Silver model follows the same pattern, established by
`events_push`:

- `materialized='incremental'`, `incremental_strategy='merge'`,
  `unique_key='id'` (mirrors the Bronze idempotency contract,
  ADR-0002)
- `on_schema_change='fail'` so an unannounced upstream change blocks
  the build rather than silently dropping columns
- Filter Bronze by `type = '<EventType>'` first, then `get_json_object`
  the specific fields the downstream marts need — nothing more
- Envelope columns (id, actor, repo, org, created_at, ingest_hour)
  carried through verbatim so cross-event-type marts can join on
  `repo_id` / `actor_id` without re-deriving them

### Rejected alternatives

1. **A single wide `silver.events` view of the union of all event
   types**. Forces every column to be typed for every event type,
   which means hundreds of mostly-null columns. Also kills the
   schema-drift containment story — one event type's drift would
   change the wide schema for everyone.
2. **A "compiled" Silver where one macro generates 15 models**. Saves
   keystrokes but obscures the per-event-type logic that an
   interviewer asking "how do you handle PR merges" will need to
   see. Reverse-engineering the macro is harder than reading 5 SQL
   files.
3. **Dynamic discovery — Silver tables auto-built when first
   referenced**. Surprise schema changes in prod from a single mart
   query. No thanks.

### dbt adapter swap plan (for Sprint 5a)

Locally: `dbt-spark` with `method: session`. On Databricks:
`dbt-databricks`. The swap will happen as part of Sprint 5a's cloud
migration. The plan:

| Step | What | When |
|------|------|------|
| 1 | Audit current macros for dbt-spark-specific behavior | end of Sprint 3 |
| 2 | Install `dbt-databricks` alongside `dbt-spark` in the venv | Sprint 5a kickoff |
| 3 | Add `prod` profile pointing at a Databricks SQL warehouse + Unity Catalog catalog | Sprint 5a |
| 4 | Run `dbt parse` against `--target prod` to surface adapter-specific errors | Sprint 5a |
| 5 | Resolve diffs one by one (most likely: `file_format` config, schema name generation, `merge` syntax differences) | Sprint 5a |
| 6 | Cut over `dev` to point at a personal Databricks workspace; keep `dbt-spark` profile as `local` for offline work | Sprint 5a |

**Macros known to need audit before swap**:

- `dbt/macros/register_external_sources.sql` — uses
  `CREATE SCHEMA IF NOT EXISTS bronze` + `CREATE TABLE ... LOCATION`.
  On Databricks the LOCATION semantics with Unity Catalog catalogs
  differ; need either an external location grant or a managed table
  rebuild on the cloud side.
- `dbt/macros/delta_source.sql` — thin abstraction over `source()`,
  should port cleanly.
- `dbt/macros/generate_schema_name.sql` — Sprint 2 added this. On
  Databricks with Unity Catalog, schemas live under a catalog
  (`catalog.schema.table`). The macro stays unchanged; the profile's
  `catalog` field handles the prefix.
- `+file_format: delta` in `dbt_project.yml` — `dbt-databricks`
  honors this; no change.

**Why not move to `dbt-databricks` now**: local dev would need a
Databricks workspace to run. The cost is "+30 seconds per dbt run"
in cold-start time and a workspace dependency for what's currently
zero-friction offline development. Defer until cloud is the
deployment target.

## Consequences

**Positive**:
- Silver grows in lockstep with real demand; no zombie tables
- Each new Silver model's design rationale is traceable to the Gold
  mart that requested it
- The adapter swap is planned, not discovered at Sprint 5a midnight

**Negative**:
- If two Gold marts in the same Sprint need the same Silver table,
  one mart owns the build and the other has an implicit dependency.
  Sprint 3a (`oss_health_mart` reading 3 Silvers) is the test case;
  document the dependency in the mart's design doc.
- An interviewer asking "where's `silver.events_member`" gets the
  answer "no mart needs it yet; payload accessible from Bronze".
  This is a defensible answer if delivered without apology.

## Status conditions for revisit

Re-open this ADR when **any one** of:

1. A second Gold mart wants a Silver table that's already
   re-implementing fields. That's the trigger to extract shared
   logic (a base model, not a macro).
2. The adapter swap surfaces a macro that needs ongoing two-adapter
   support, not a one-time port.
3. Bronze grows past ~10 partitions per day and the per-event-type
   scan starts hurting — at which point per-type pre-partitioning
   in Bronze becomes a question.

## References

- ADR-0001 (Bronze payload handling) — established that payload is
  raw JSON in Bronze, typed only at Silver
- ADR-0002 (event_id idempotency) — Silver inherits this contract
- ADR-0003 (partition by ingest_hour) — Silver incremental cutoff
  uses `ingest_hour`, mirroring Bronze
- `dbt/models/silver/events_push.sql` — Sprint 1 prototype that all
  later Silver models follow
