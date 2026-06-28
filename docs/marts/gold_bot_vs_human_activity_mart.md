# Gold mart: `bot_vs_human_activity_mart`

- **Owner**: Peter Wang
- **Status**: Sprint 3b in flight
- **Source layer**: Silver — all 6 event-type tables
  (`events_push`, `events_pull_request`, `events_issues`,
  `events_issue_comment`, `events_watch`, `events_fork`)
- **Bot rule**: ADR-0006 (Rule A `[bot]` suffix + Rule C
  `known_bots` allowlist, via `{{ is_bot('actor_login') }}` macro)
- **Materialization**: dbt incremental, merge on composite key
- **dbt target file**:
  `dbt/models/gold/bot_vs_human_activity_mart.sql`

## Purpose

Answers two questions a CTO / OSS funder / vendor would ask:

1. *"How much of this repo's daily traffic is automated?"*
2. *"Is that share growing or shrinking?"*

And a third question for the engineering side:

3. *"Which event types are the bot signal coming from?"*
   (PushEvent vs IssueCommentEvent vs WatchEvent — the answer differs
   a lot by repo culture.)

## Grain

**One row per `(repo_id, activity_date)`.** Same as the two prior
marts so a single dashboard can join all three.

`activity_date` is `DATE(created_at)` of the *event* (the moment the
bot/human acted). Consistent with `oss_health_mart`.

## Source unification strategy

Each of the 6 Silver tables contributes one row per event. The mart
unions them into a long table with a single `event_class` column
(`push` / `pr` / `issue` / `comment` / `watch` / `fork`), then
aggregates per repo / date. This lets per-class counts share one
group-by pass.

```sql
unified as (
    select repo_id, actor_id, actor_login, created_at, 'push'    as event_class from {{ ref('events_push') }}
    union all
    select repo_id, actor_id, actor_login, created_at, 'pr'      as event_class from {{ ref('events_pull_request') }}
    union all
    select repo_id, actor_id, actor_login, created_at, 'issue'   as event_class from {{ ref('events_issues') }}
    union all
    select repo_id, actor_id, actor_login, created_at, 'comment' as event_class from {{ ref('events_issue_comment') }}
    union all
    select repo_id, actor_id, actor_login, created_at, 'watch'   as event_class from {{ ref('events_watch') }}
    union all
    select repo_id, actor_id, actor_login, created_at, 'fork'    as event_class from {{ ref('events_fork') }}
)
```

`event_class` is intentionally a short token, not a full event type
name, because it becomes a column suffix in the pivoted output and
shorter is friendlier.

## Metrics

### Total counts

| Column | Definition |
|--------|-----------|
| `event_count` | Total events for the repo/day (all 6 types unioned) |
| `bot_event_count` | Events where `is_bot(actor_login)` is true |
| `human_event_count` | Events where `is_bot(actor_login)` is false AND `actor_login` is non-null |
| `bot_event_share` | `bot_event_count / event_count` |

### Per-event-class bot count

| Column | Definition |
|--------|-----------|
| `push_bot_count` | bot events where `event_class='push'` |
| `pr_bot_count` | bot events where `event_class='pr'` |
| `issue_bot_count` | bot events where `event_class='issue'` |
| `comment_bot_count` | bot events where `event_class='comment'` |
| `watch_bot_count` | bot events where `event_class='watch'` |
| `fork_bot_count` | bot events where `event_class='fork'` |

(For non-bot per-class counts, subtract the bot count from the same
class total in `repo_daily_activity` / `oss_health_mart`. The bot
mart doesn't repeat that information.)

### Distinct actor counts

| Column | Definition |
|--------|-----------|
| `distinct_bot_actors` | `count(distinct actor_id)` where `is_bot` |
| `distinct_human_actors` | `count(distinct actor_id)` where NOT `is_bot` |

### App-event side-channel

| Column | Definition |
|--------|-----------|
| `app_event_count` | `events_issue_comment` rows on this repo/day where `is_app_event` (the corrected `performed_via_github_app` signal) |

This is intentionally separated from bot counts per ADR-0006. App
events include humans using GitHub Apps; conflating them with bot
events would silently overcount automation.

## Idempotency

Composite-key merge on `(repo_id, activity_date)`. Same
late-arrival caveat as the other marts (deferred to Sprint 4 DAG
design).

## Verification (Sprint 3b)

After build:

1. Grain invariant: `count(*) == count(distinct repo_id, activity_date)`.
2. Cross-mart check: `bot_event_count + human_event_count + null_actor_events == event_count`
   for every row (where `null_actor_events` is the difference). This
   is enforced as an expression test in `_gold_schema.yml`.
3. Bot recall sanity: confirm `github-actions[bot]` appears in
   `bot_event_count` for repos where it pushed (it's the largest
   bot in the sample).

## Tests (`_gold_schema.yml`)

- Composite-grain uniqueness
- `not_null` on grain + count columns
- `*_count >= 0`
- `bot_event_count + human_event_count <= event_count`
  (the slack is the rare `actor_login is null` row)
- `bot_event_share = bot_event_count / nullif(event_count, 0)`

## Out of scope

- Time-of-day bot patterns (would need finer than daily grain)
- Per-bot leaderboards (this is a repo mart, not an actor mart)
- "Uncertain bucket" detection — deferred per ADR-0006 future work

## Why this mart matters

`repo_daily_activity` says how busy a repo is. `oss_health_mart` says
how healthy. `bot_vs_human_activity_mart` says how *real* the busyness
is — a key qualifier for ranking repos or assessing OSS-funding signal.
