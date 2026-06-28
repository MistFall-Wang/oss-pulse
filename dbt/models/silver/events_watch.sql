{{
    config(
        materialized='incremental',
        unique_key='id',
        incremental_strategy='merge',
        file_format='delta',
        on_schema_change='fail'
    )
}}

{# WatchEvent in GH Archive represents starring a repo (GitHub renamed
   "watch" to "star" in the UI long ago, but the event type kept the
   old name). The payload is minimal (just `action`, currently always
   'started'), so the row is almost entirely envelope columns. That's
   on purpose — this Silver table exists to make star events joinable
   with the rest of Silver, not to add new semantics.
#}

with bronze_watch as (
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
    where type = 'WatchEvent'

    {% if is_incremental() %}
        and ingest_hour > (select coalesce(max(ingest_hour), '1970-01-01-00') from {{ this }})
    {% endif %}
)

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
    get_json_object(payload_raw, '$.action') as action
from bronze_watch
