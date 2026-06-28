# Gold mart: `oss_health_mart`

- **Owner**: Peter Wang
- **Status**: Sprint 3a in flight
- **Source layer**: `silver.events_pull_request`,
  `silver.events_issues`, `silver.events_issue_comment`
- **Materialization**: dbt incremental, merge on composite key
- **dbt target file**: `dbt/models/gold/oss_health_mart.sql`

## Purpose

Answers the engineering-manager question:
*"Is this repo healthy, and how does that change over time?"*

Concretely:

- How fast does this repo merge PRs?
- How fast does this repo close issues?
- How quickly do maintainers respond to new issues?
- How active is the comment thread?
- How many distinct contributors interact with the repo per day?

The mart is the bridge between raw activity counts
(`repo_daily_activity`) and project-quality signals OSS funders /
recruiters / vendors care about. It is intentionally daily-grain so
downstream visualizations (Sprint 5+) can show health-trend lines.

## Grain

**One row per `(repo_id, activity_date)`.**

`activity_date` is the date the *response/closure/comment* event
happened, not the date the original issue/PR was opened. Rationale:
the mart is the time series of *how this repo behaved on day D*, not
"all PRs opened on day D, eventually merged". An open PR from 30 days
ago that got merged today contributes to *today's* merge metrics.

## Source events and what each contributes

| Silver source | Contributes |
|---------------|-------------|
| `events_pull_request` | `pr_opened_count`, `pr_closed_count`, `pr_merged_count`, `pr_avg_merge_latency_hours` |
| `events_issues` | `issue_opened_count`, `issue_closed_count`, `issue_avg_close_latency_hours` |
| `events_issue_comment` | `issue_comment_count`, `issue_avg_first_response_hours`, contributes to `unique_contributors` |

`unique_contributors` is the count-distinct of `actor_id` unioned
across all three sources for that repo/day.

## Metrics

### PR metrics

| Column | Source | Definition |
|--------|--------|-----------|
| `pr_opened_count` | `events_pull_request` | events where `action='opened'`, grouped by `DATE(created_at)` |
| `pr_closed_count` | same | `action='closed'`, regardless of merged |
| `pr_merged_count` | same | `action='closed' AND pr_merged=true` |
| `pr_avg_merge_latency_hours` | same | for events with `pr_merged=true`, `avg(unix(pr_merged_at) - unix(pr_created_at)) / 3600`. Latency uses fields on the payload itself, so PRs opened weeks ago still get a correct latency the day they merge. |

### Issue metrics

| Column | Source | Definition |
|--------|--------|-----------|
| `issue_opened_count` | `events_issues` | `action='opened'`, grouped by `DATE(created_at)` of the event |
| `issue_closed_count` | same | `action='closed'` |
| `issue_avg_close_latency_hours` | same | for `action='closed'`, `avg(unix(issue_closed_at) - unix(issue_created_at)) / 3600` |

### Comment + responsiveness

| Column | Source | Definition |
|--------|--------|-----------|
| `issue_comment_count` | `events_issue_comment` | `action='created'`, grouped by `DATE(comment_created_at)` |
| `issue_avg_first_response_hours` | same | per-issue, find the earliest comment from a user that is **not** the issue opener, after `issue_created_at`. Compute `unix(first_response) - unix(issue_created_at)`. Group by `DATE(first_response)`, take `avg`. |

### Cross-event

| Column | Source | Definition |
|--------|--------|-----------|
| `unique_contributors` | all three | `count(distinct actor_id)` from a UNION of the three Silver tables filtered to the same `(repo_id, activity_date)` |

## Idempotency contract

- Incremental strategy: `merge` on composite `(repo_id, activity_date)`
- Same shape as `repo_daily_activity` (Sprint 2). Same composite-key
  invariant test in `_gold_schema.yml`.
- Same late-arrival caveat (documented in
  `gold_repo_daily_activity.md` and not re-solved here — Sprint 4's
  Airflow DAG will handle it for both marts together).

## Known sample-window limitations

1. **`issue_avg_first_response_hours` undercounts old issues**: if the
   first non-opener comment to an issue happened in an ingest_hour
   outside our 4-hour sample window, we don't see it, and a
   later-comment shows up as the apparent "first response". The mart
   correctly reflects "first response we observed", which is what's
   provable from the data. The design doc records this honestly
   rather than silently overestimating responsiveness.
2. **PR merge latency for never-merged PRs is null**: by design — a
   never-merged PR has no merge timestamp. Closed-without-merge is
   captured in `pr_closed_count - pr_merged_count`.

## Tests (`_gold_schema.yml`)

- `dbt_utils.unique_combination_of_columns([repo_id, activity_date])`
- `not_null` on grain columns and all `*_count` columns
- `*_count >= 0` expression tests (counts are never negative)
- `pr_merged_count <= pr_closed_count` expression test (merge is a
  special case of close)

## Verification (Sprint 3a step 6)

After build:

1. Grain invariant via `gold_health_verify.py` and the dbt schema test
2. Cross-check: pick a PR in the sample that was merged in
   2025-01-15-12 or -13. Manually compute merge latency from
   `pr_created_at` / `pr_merged_at` straight from
   `silver.events_pull_request`, confirm the mart's per-day average
   for that repo on 2025-01-15 reflects it.

## Out of scope

- PR review latency (would need `PullRequestReviewEvent`, deferred per
  ADR-0005 tier 4)
- PR throughput per author (this mart is repo-scoped; an
  author-scoped mart is separate work)
- Stale-PR backlog (requires holding state for multiple days — better
  as a downstream window function over this mart, not in the mart)

## Why this mart matters

`repo_daily_activity` answers *"who is busy"*. `oss_health_mart`
answers *"who is healthy"*. A repo can be busy and unhealthy (e.g.
high push count but unmerged PRs piling up) or quiet and healthy
(small but-fast project). The two together are the foundation for
the bot mart (Sprint 3b) and the future Repo Growth anomaly
detection (deferred).
