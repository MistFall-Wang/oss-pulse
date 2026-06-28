{{
    config(
        materialized='incremental',
        unique_key='id',
        incremental_strategy='merge',
        file_format='delta',
        on_schema_change='fail'
    )
}}

with bronze_push as (
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
    where type = 'PushEvent'

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
        cast(get_json_object(payload_raw, '$.push_id')       as bigint) as push_id,
        -- Fix for incident-0001: payload.size renamed to payload.commit_count
        -- on some 2099-vintage hours. coalesce both names so historic and
        -- post-rename rows both populate commit_size. See
        -- docs/postmortems/0001-schema-drift.md for the incident path.
        coalesce(
            cast(get_json_object(payload_raw, '$.size')         as int),
            cast(get_json_object(payload_raw, '$.commit_count') as int)
        )                                                                  as commit_size,
        cast(get_json_object(payload_raw, '$.distinct_size') as int)    as distinct_commit_size,
                get_json_object(payload_raw, '$.ref')                   as ref,
                get_json_object(payload_raw, '$.head')                  as head_sha,
                get_json_object(payload_raw, '$.before')                as before_sha
    from bronze_push
)

select * from parsed
