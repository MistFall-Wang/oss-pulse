{{
    config(
        materialized='incremental',
        unique_key=['repo_id', 'activity_date'],
        incremental_strategy='merge',
        file_format='delta',
        on_schema_change='fail'
    )
}}

{# Union all 6 Silver event-type tables into a (repo, date, class)
   long form, then aggregate per repo/date. Bot classification is
   centralised in the is_bot() macro (ADR-0006). is_app_event lives
   only on comment events for now (Sprint 3a) and is reported as a
   separate metric, not folded into bot counts.
#}

with unified as (
    select repo_id, actor_id, actor_login, created_at, 'push' as event_class
    from {{ ref('events_push') }}
    {% if is_incremental() %}
        where cast(created_at as date) > (select coalesce(max(activity_date), date('1970-01-01')) from {{ this }})
    {% endif %}

    union all

    select repo_id, actor_id, actor_login, created_at, 'pr' as event_class
    from {{ ref('events_pull_request') }}
    {% if is_incremental() %}
        where cast(created_at as date) > (select coalesce(max(activity_date), date('1970-01-01')) from {{ this }})
    {% endif %}

    union all

    select repo_id, actor_id, actor_login, created_at, 'issue' as event_class
    from {{ ref('events_issues') }}
    {% if is_incremental() %}
        where cast(created_at as date) > (select coalesce(max(activity_date), date('1970-01-01')) from {{ this }})
    {% endif %}

    union all

    select repo_id, actor_id, actor_login, created_at, 'comment' as event_class
    from {{ ref('events_issue_comment') }}
    {% if is_incremental() %}
        where cast(created_at as date) > (select coalesce(max(activity_date), date('1970-01-01')) from {{ this }})
    {% endif %}

    union all

    select repo_id, actor_id, actor_login, created_at, 'watch' as event_class
    from {{ ref('events_watch') }}
    {% if is_incremental() %}
        where cast(created_at as date) > (select coalesce(max(activity_date), date('1970-01-01')) from {{ this }})
    {% endif %}

    union all

    select source_repo_id as repo_id, actor_id, actor_login, created_at, 'fork' as event_class
    from {{ ref('events_fork') }}
    {% if is_incremental() %}
        where cast(created_at as date) > (select coalesce(max(activity_date), date('1970-01-01')) from {{ this }})
    {% endif %}
),

classified as (
    select
        repo_id,
        cast(created_at as date) as activity_date,
        actor_id,
        actor_login,
        event_class,
        case when {{ is_bot('actor_login') }} then 1 else 0 end as is_bot_flag
    from unified
),

aggregated as (
    select
        repo_id,
        activity_date,
        count(*)                                                              as event_count,
        sum(is_bot_flag)                                                      as bot_event_count,
        sum(case when is_bot_flag = 0 and actor_login is not null then 1 else 0 end) as human_event_count,

        sum(case when is_bot_flag = 1 and event_class = 'push'    then 1 else 0 end) as push_bot_count,
        sum(case when is_bot_flag = 1 and event_class = 'pr'      then 1 else 0 end) as pr_bot_count,
        sum(case when is_bot_flag = 1 and event_class = 'issue'   then 1 else 0 end) as issue_bot_count,
        sum(case when is_bot_flag = 1 and event_class = 'comment' then 1 else 0 end) as comment_bot_count,
        sum(case when is_bot_flag = 1 and event_class = 'watch'   then 1 else 0 end) as watch_bot_count,
        sum(case when is_bot_flag = 1 and event_class = 'fork'    then 1 else 0 end) as fork_bot_count,

        count(distinct case when is_bot_flag = 1 then actor_id end) as distinct_bot_actors,
        count(distinct case when is_bot_flag = 0 then actor_id end) as distinct_human_actors
    from classified
    group by repo_id, activity_date
),

app_events as (
    select
        repo_id,
        cast(created_at as date) as activity_date,
        count(*) as app_event_count
    from {{ ref('events_issue_comment') }}
    where is_app_event = true
    {% if is_incremental() %}
        and cast(created_at as date) > (select coalesce(max(activity_date), date('1970-01-01')) from {{ this }})
    {% endif %}
    group by repo_id, cast(created_at as date)
)

select
    a.repo_id,
    a.activity_date,
    a.event_count,
    a.bot_event_count,
    a.human_event_count,
    case when a.event_count > 0
         then cast(a.bot_event_count as double) / a.event_count
         else 0.0 end                              as bot_event_share,
    a.push_bot_count,
    a.pr_bot_count,
    a.issue_bot_count,
    a.comment_bot_count,
    a.watch_bot_count,
    a.fork_bot_count,
    a.distinct_bot_actors,
    a.distinct_human_actors,
    coalesce(ae.app_event_count, 0)                as app_event_count
from aggregated a
left join app_events ae using (repo_id, activity_date)
