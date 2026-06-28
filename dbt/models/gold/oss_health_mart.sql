{{
    config(
        materialized='incremental',
        unique_key=['repo_id', 'activity_date'],
        incremental_strategy='merge',
        file_format='delta',
        on_schema_change='fail'
    )
}}

{# Build the (repo_id, activity_date) universe by unioning what each
   source touches on a given day, then left-join the per-source metrics
   onto it. This keeps a repo on the mart for any day it had any signal,
   even if (say) it had comments but no PR activity that day.
#}

with pr_events as (
    select
        repo_id,
        actor_id,
        cast(created_at as date) as activity_date,
        action,
        pr_id,
        pr_merged,
        pr_created_at,
        pr_merged_at
    from {{ ref('events_pull_request') }}

    {% if is_incremental() %}
        where cast(created_at as date) > (
            select coalesce(max(activity_date), date('1970-01-01')) from {{ this }}
        )
    {% endif %}
),

issue_events as (
    select
        repo_id,
        actor_id,
        cast(created_at as date) as activity_date,
        action,
        issue_id,
        issue_created_at,
        issue_closed_at
    from {{ ref('events_issues') }}

    {% if is_incremental() %}
        where cast(created_at as date) > (
            select coalesce(max(activity_date), date('1970-01-01')) from {{ this }}
        )
    {% endif %}
),

comment_events as (
    select
        repo_id,
        actor_id,
        issue_id,
        issue_opener_user_id,
        issue_created_at,
        comment_user_id,
        comment_created_at,
        cast(comment_created_at as date) as activity_date
    from {{ ref('events_issue_comment') }}
    where action = 'created'
      and comment_created_at is not null

    {% if is_incremental() %}
        and cast(comment_created_at as date) > (
            select coalesce(max(activity_date), date('1970-01-01')) from {{ this }}
        )
    {% endif %}
),

pr_agg as (
    select
        repo_id,
        activity_date,
        sum(case when action = 'opened' then 1 else 0 end) as pr_opened_count,
        sum(case when action = 'closed' then 1 else 0 end) as pr_closed_count,
        sum(case when action = 'closed' and pr_merged then 1 else 0 end) as pr_merged_count,
        avg(case when pr_merged and pr_merged_at is not null and pr_created_at is not null
                 then (unix_timestamp(pr_merged_at) - unix_timestamp(pr_created_at)) / 3600.0 end
        ) as pr_avg_merge_latency_hours
    from pr_events
    group by repo_id, activity_date
),

issue_agg as (
    select
        repo_id,
        activity_date,
        sum(case when action = 'opened' then 1 else 0 end) as issue_opened_count,
        sum(case when action = 'closed' then 1 else 0 end) as issue_closed_count,
        avg(case when action = 'closed' and issue_closed_at is not null and issue_created_at is not null
                 then (unix_timestamp(issue_closed_at) - unix_timestamp(issue_created_at)) / 3600.0 end
        ) as issue_avg_close_latency_hours
    from issue_events
    group by repo_id, activity_date
),

comment_count_agg as (
    select
        repo_id,
        activity_date,
        count(*) as issue_comment_count
    from comment_events
    group by repo_id, activity_date
),

{# First-response-time: per (repo_id, issue_id), the earliest
   comment whose author is NOT the issue opener. Attribute the
   latency to the date of that first response.
#}
first_response as (
    select
        repo_id,
        issue_id,
        issue_created_at,
        min(case when comment_user_id != issue_opener_user_id and comment_user_id is not null
                 then comment_created_at end) as first_response_at
    from comment_events
    where issue_created_at is not null
    group by repo_id, issue_id, issue_created_at
),

response_agg as (
    select
        repo_id,
        cast(first_response_at as date) as activity_date,
        avg((unix_timestamp(first_response_at) - unix_timestamp(issue_created_at)) / 3600.0)
            as issue_avg_first_response_hours
    from first_response
    where first_response_at is not null
      and first_response_at >= issue_created_at
    group by repo_id, cast(first_response_at as date)
),

contributor_union as (
    select repo_id, activity_date, actor_id from pr_events
    union all
    select repo_id, activity_date, actor_id from issue_events
    union all
    select repo_id, activity_date, actor_id from comment_events
),

contributor_agg as (
    select
        repo_id,
        activity_date,
        count(distinct actor_id) as unique_contributors
    from contributor_union
    group by repo_id, activity_date
),

key_union as (
    select repo_id, activity_date from pr_agg
    union
    select repo_id, activity_date from issue_agg
    union
    select repo_id, activity_date from comment_count_agg
    union
    select repo_id, activity_date from response_agg
)

select
    k.repo_id,
    k.activity_date,
    coalesce(p.pr_opened_count, 0)                as pr_opened_count,
    coalesce(p.pr_closed_count, 0)                as pr_closed_count,
    coalesce(p.pr_merged_count, 0)                as pr_merged_count,
    p.pr_avg_merge_latency_hours,
    coalesce(i.issue_opened_count, 0)             as issue_opened_count,
    coalesce(i.issue_closed_count, 0)             as issue_closed_count,
    i.issue_avg_close_latency_hours,
    coalesce(c.issue_comment_count, 0)            as issue_comment_count,
    r.issue_avg_first_response_hours,
    coalesce(co.unique_contributors, 0)           as unique_contributors
from key_union k
left join pr_agg            p  using (repo_id, activity_date)
left join issue_agg         i  using (repo_id, activity_date)
left join comment_count_agg c  using (repo_id, activity_date)
left join response_agg      r  using (repo_id, activity_date)
left join contributor_agg   co using (repo_id, activity_date)
