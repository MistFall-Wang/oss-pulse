{{
    config(
        materialized='incremental',
        unique_key='id',
        incremental_strategy='merge',
        file_format='delta',
        on_schema_change='fail'
    )
}}

with bronze_fork as (
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
    where type = 'ForkEvent'

    {% if is_incremental() %}
        and ingest_hour > (select coalesce(max(ingest_hour), '1970-01-01-00') from {{ this }})
    {% endif %}
)

select
    id,
    actor_id,
    actor_login,
    repo_id          as source_repo_id,
    repo_name        as source_repo_name,
    org_id,
    org_login,
    is_public,
    created_at,
    ingest_hour,
    cast(get_json_object(payload_raw, '$.forkee.id')   as bigint) as forkee_repo_id,
         get_json_object(payload_raw, '$.forkee.full_name')        as forkee_repo_name,
    cast(get_json_object(payload_raw, '$.forkee.owner.id') as bigint) as forkee_owner_id,
         get_json_object(payload_raw, '$.forkee.owner.login')      as forkee_owner_login
from bronze_fork
