{{
    config(
        materialized='incremental',
        unique_key='id',
        incremental_strategy='merge',
        file_format='delta',
        on_schema_change='fail'
    )
}}

with bronze_pr as (
    select
        id,
        actor_id,
        actor_login,
        repo_id,
        repo_name,
        org_id,
        org_login,
        is_public,
        created_at,
        ingest_hour,
        payload_raw
    from {{ delta_source('bronze', 'events') }}
    where type = 'PullRequestEvent'

    {% if is_incremental() %}
        and ingest_hour > (select coalesce(max(ingest_hour), '1970-01-01-00') from {{ this }})
    {% endif %}
),

parsed as (
    select
        id,
        actor_id,
        actor_login,
        repo_id,
        repo_name,
        org_id,
        org_login,
        is_public,
        created_at,
        ingest_hour,

        get_json_object(payload_raw, '$.action')                               as action,
        cast(get_json_object(payload_raw, '$.number')              as bigint)  as pr_number,
        cast(get_json_object(payload_raw, '$.pull_request.id')     as bigint)  as pr_id,
                get_json_object(payload_raw, '$.pull_request.state')           as pr_state,
        cast(get_json_object(payload_raw, '$.pull_request.merged') as boolean) as pr_merged,
        to_timestamp(get_json_object(payload_raw, '$.pull_request.created_at'), "yyyy-MM-dd'T'HH:mm:ss'Z'")  as pr_created_at,
        to_timestamp(get_json_object(payload_raw, '$.pull_request.closed_at'),  "yyyy-MM-dd'T'HH:mm:ss'Z'")  as pr_closed_at,
        to_timestamp(get_json_object(payload_raw, '$.pull_request.merged_at'),  "yyyy-MM-dd'T'HH:mm:ss'Z'")  as pr_merged_at,
        cast(get_json_object(payload_raw, '$.pull_request.user.id') as bigint) as pr_user_id,
                get_json_object(payload_raw, '$.pull_request.user.login')      as pr_user_login,
        cast(get_json_object(payload_raw, '$.pull_request.commits')      as int) as pr_commits,
        cast(get_json_object(payload_raw, '$.pull_request.additions')    as int) as pr_additions,
        cast(get_json_object(payload_raw, '$.pull_request.deletions')    as int) as pr_deletions,
        cast(get_json_object(payload_raw, '$.pull_request.changed_files') as int) as pr_changed_files
    from bronze_pr
)

select * from parsed
