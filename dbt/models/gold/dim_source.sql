{{ config(materialized='table') }}

with distinct_sources as (
    select distinct source
    from {{ ref('silver_weather_events') }}
)

select
    row_number() over (order by source) as source_key,
    source as source_name
from distinct_sources
