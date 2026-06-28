{{
    config(
        materialized='incremental',
        unique_key=['repo_id', 'activity_date'],
        incremental_strategy='merge',
        file_format='delta',
        on_schema_change='fail'
    )
}}

with push as (
    select
        repo_id,
        repo_name,
        org_id,
        org_login,
        actor_id,
        actor_login,
        id           as event_id,
        commit_size,
        distinct_commit_size,
        cast(created_at as date) as activity_date
    from {{ ref('events_push') }}

    {% if is_incremental() %}
        where cast(created_at as date) > (
            select coalesce(max(activity_date), date('1970-01-01'))
            from {{ this }}
        )
    {% endif %}
),

aggregated as (
    select
        repo_id,
        activity_date,
        max(repo_name)       as repo_name,
        max(org_id)          as org_id,
        max(org_login)       as org_login,
        count(event_id)      as push_count,
        sum(commit_size)     as total_commits,
        sum(distinct_commit_size) as distinct_commits,
        count(distinct actor_id)  as unique_pushers,
        sum(case when {{ is_bot('actor_login') }} then 1 else 0 end) as bot_push_count
    from push
    group by repo_id, activity_date
)

select
    repo_id,
    activity_date,
    repo_name,
    org_id,
    org_login,
    push_count,
    total_commits,
    distinct_commits,
    unique_pushers,
    bot_push_count,
    push_count - bot_push_count as non_bot_push_count
from aggregated
