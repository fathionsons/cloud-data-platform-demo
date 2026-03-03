{{ config(materialized='table') }}

with events as (
    select *
    from {{ ref('silver_weather_events') }}
)

select
    md5(
        events.location_name
        || '|'
        || cast(events.observation_ts_utc as varchar)
        || '|'
        || events.source
    ) as observation_key,
    cast(strftime(cast(events.observation_ts_utc as date), '%Y%m%d') as integer) as date_key,
    dim_location.location_key,
    dim_source.source_key,
    events.observation_ts_utc,
    events.temperature_c,
    events.apparent_temperature_c,
    events.precipitation_mm,
    events.wind_speed_kmh
from events
inner join {{ ref('dim_location') }} as dim_location
    on events.location_name = dim_location.location_name
    and events.latitude = dim_location.latitude
    and events.longitude = dim_location.longitude
inner join {{ ref('dim_source') }} as dim_source
    on events.source = dim_source.source_name
