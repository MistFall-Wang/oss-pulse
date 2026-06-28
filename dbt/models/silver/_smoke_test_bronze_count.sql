{{ config(materialized='table') }}

select
    ingest_hour,
    count(*) as event_count,
    count(distinct id) as unique_ids,
    count(distinct type) as distinct_types
from {{ delta_source('bronze', 'events') }}
group by ingest_hour
order by ingest_hour