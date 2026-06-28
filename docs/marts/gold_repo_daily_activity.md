# Gold mart: `repo_daily_activity`

- **Owner**: Peter Wang
- **Status**: Sprint 2 in flight
- **Source layer**: `silver.events_push`
- **Materialization**: dbt incremental, merge on composite key
- **dbt target file**: `dbt/models/gold/repo_daily_activity.sql`

## Purpose

First business-facing artifact of the lakehouse. Answers questions
like:

- "Which repos pushed the most code on a given day?"
- "Which repos have a sudden spike in push activity in the last week?"
  (downstream — Repo Growth Mart in Sprint 3 will layer 7-day anomaly
  detection on top of this table.)
- "How much of a repo's push traffic comes from bots?"

This mart is intentionally narrow: PushEvent-only. The other event
types feed `oss_health_mart` and `bot_vs_human_activity_mart` in
Sprint 3.

## Grain

**One row per `(repo_id, activity_date)`.**

- `activity_date` is `DATE(created_at)` — the *event* date, not the
  ingest hour. Two reasons:
  1. A push that happens at 23:59 UTC and lands in ingest_hour `00`
     of the next day still belongs to the day the push happened, not
     the day it was ingested.
  2. The mart is the day-by-day business story; ingest_hour is
     plumbing.
- Using `(repo_id, activity_date)` and **not** `(repo_name, activity_date)`
  is deliberate. `repo_name` can change (renames, transfers); `repo_id`
  is stable. ADR-0004 codifies this for the project.

## Metrics

| Column | Type | Definition |
|--------|------|-----------|
| `push_count` | bigint | Count of distinct `PushEvent.id` for the repo/day |
| `total_commits` | bigint | `sum(commit_size)` — every commit in every push, including merges and duplicates |
| `distinct_commits` | bigint | `sum(distinct_commit_size)` — commits that are not already in any other ref of the same repo at push time (GitHub's `distinct_size`) |
| `unique_pushers` | bigint | `count(distinct actor_id)` |
| `bot_push_count` | bigint | `push_count` where `actor_login` ends with `[bot]` (Rule A from the Sprint 2.5 spike). Rules B/C are deferred to ADR-0006 / Sprint 3 because they don't apply meaningfully to PushEvent in the current sample. |
| `non_bot_push_count` | bigint | `push_count - bot_push_count` |

### Dimensional columns kept on the mart

These are denormalized for now (no `dim_repo`). They make the mart
self-serve in ad-hoc queries.

| Column | Source | Notes |
|--------|--------|-------|
| `repo_name` | most recent `silver.events_push.repo_name` for the repo on that day | takes `MAX(repo_name)` per group — fine because renames within a single UTC day are extremely rare and stale `repo_name` here doesn't break joins (those use `repo_id`) |
| `org_id` | most recent value | nullable — many repos are user-owned |
| `org_login` | most recent value | nullable |

## Idempotency contract

- Incremental strategy: `merge`
- `unique_key`: `(repo_id, activity_date)` (composite — uses dbt-utils
  helper or string concatenation; see implementation note)
- Re-running the mart on overlapping input must:
  - Produce the same row count for already-seen days
  - Update metric columns deterministically (Silver is itself
    deterministic from Bronze; Bronze is deterministic from source
    files — chain of trust held)

**Verifiable invariant**:
`count(*) == dbt_utils.unique_combination_of_columns(repo_id, activity_date)`
This is enforced in `_gold_schema.yml`. Sprint 2 step 4 also runs an
ad-hoc invariant check after the build (see runbook in
`spark/jobs/bronze_inspect.py` for the pattern).

## Incremental cutoff

Same shape as `silver.events_push`:

```sql
{% if is_incremental() %}
    where activity_date > (select coalesce(max(activity_date), date('1970-01-01')) from {{ this }})
{% endif %}
```

**Caveat for late-arriving events**: a PushEvent created at 23:59 UTC
on day D that lands in an ingest_hour processed on day D+1 would have
`activity_date = D`, but the incremental cutoff would have already
moved past D and the new row would be silently dropped.

Sprint 2 punts on this because:
- The Sprint 1 sample is point-in-time hourly files with no
  late-arrival risk.
- A proper fix (look-back window, or reprocess last 2 days) belongs
  with the Airflow DAG design in Sprint 4.

The design doc records this as a known limitation. Sprint 4 will
either widen the cutoff to `activity_date >= max - interval 1 day` or
require the DAG to re-run yesterday's partition.

## Ground-truth validation (Sprint 2 step 4)

No GitHub API token in dev. Validation strategy is **layer
cross-check**, not external API:

1. Pick a known-active repo present in the 2025 sample (e.g.
   `microsoft/vscode` or similar — repo TBD at run time based on
   Bronze contents).
2. Run two independent counts for one `activity_date`:
   - From the mart: `select push_count, total_commits from gold.repo_daily_activity where repo_id = X and activity_date = Y`
   - From Silver directly: `select count(*), sum(commit_size) from silver.events_push where repo_id = X and date(created_at) = Y`
3. They must be equal. If not, the mart's aggregation logic is wrong.
4. Document the result in the Sprint 2 commit message.

This is intentionally not a GitHub API check; that's deferred until
Sprint 3 when a token will be needed for other reasons.

## Tests (`_gold_schema.yml`)

- `dbt_utils.unique_combination_of_columns([repo_id, activity_date])`
- `not_null` on `repo_id`, `activity_date`, `push_count`,
  `total_commits`, `bot_push_count`, `non_bot_push_count`
- `push_count >= bot_push_count` — accepted-values or expression test
- `non_bot_push_count = push_count - bot_push_count` — relationships
  test or custom

## Out of scope for this mart

- Star / fork / PR / issue activity → Sprint 3 marts
- Anomaly detection (7-day spike) → Sprint 3 Repo Growth Mart, which
  reads from this table
- `dim_repo` → not warranted until a second mart needs repo
  attributes denormalized differently (YAGNI; see working principle 6)

## Why this mart first

It's the smallest mart that proves the full Bronze → Silver → Gold
chain works *and* produces a metric a non-engineer would care about
("which repos shipped the most code today"). Once it's green, every
later mart is a variant of the same pattern.
