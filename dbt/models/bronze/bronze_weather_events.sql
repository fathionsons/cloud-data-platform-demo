{{ config(materialized='view') }}

with raw_events as (
    select *
    from read_json_auto(
        '{{ var("bronze_glob") }}',
        format='newline_delimited',
        union_by_name=true
    )
)

select
    cast(source as varchar) as source,
    cast(fetched_at as timestamp) as fetched_at_utc,
    cast(location_name as varchar) as location_name,
    cast(latitude as double) as latitude,
    cast(longitude as double) as longitude,
    cast(timezone as varchar) as timezone,
    cast(observation_time as timestamp) as observation_ts_utc,
    cast(temperature_2m as double) as temperature_c,
    cast(apparent_temperature as double) as apparent_temperature_c,
    cast(precipitation as double) as precipitation_mm,
    cast(wind_speed_10m as double) as wind_speed_kmh
from raw_events
