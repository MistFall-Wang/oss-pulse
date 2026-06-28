{{
    config(
        materialized='incremental',
        unique_key='id',
        incremental_strategy='merge',
        file_format='delta',
        on_schema_change='fail'
    )
}}

with bronze_issues as (
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
    where type = 'IssuesEvent'

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

        get_json_object(payload_raw, '$.action')                              as action,
        cast(get_json_object(payload_raw, '$.issue.id')           as bigint)  as issue_id,
        cast(get_json_object(payload_raw, '$.issue.number')       as bigint)  as issue_number,
                get_json_object(payload_raw, '$.issue.state')                 as issue_state,
        cast(get_json_object(payload_raw, '$.issue.user.id')      as bigint)  as issue_user_id,
                get_json_object(payload_raw, '$.issue.user.login')            as issue_user_login,
        to_timestamp(get_json_object(payload_raw, '$.issue.created_at'), "yyyy-MM-dd'T'HH:mm:ss'Z'") as issue_created_at,
        to_timestamp(get_json_object(payload_raw, '$.issue.closed_at'),  "yyyy-MM-dd'T'HH:mm:ss'Z'") as issue_closed_at,
        cast(get_json_object(payload_raw, '$.issue.comments') as int) as issue_comments_count
    from bronze_issues
)

select * from parsed
