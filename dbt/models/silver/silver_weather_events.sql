{{ config(materialized='table') }}

with ranked as (
    select
        source,
        fetched_at_utc,
        location_name,
        latitude,
        longitude,
        timezone,
        observation_ts_utc,
        temperature_c,
        apparent_temperature_c,
        precipitation_mm,
        wind_speed_kmh,
        row_number() over (
            partition by location_name, observation_ts_utc
            order by fetched_at_utc desc
        ) as row_num
    from {{ ref('bronze_weather_events') }}
    where observation_ts_utc is not null
)

select
    md5(location_name || '|' || cast(observation_ts_utc as varchar)) as observation_nk,
    source,
    fetched_at_utc,
    location_name,
    latitude,
    longitude,
    timezone,
    observation_ts_utc,
    temperature_c,
    apparent_temperature_c,
    precipitation_mm,
    wind_speed_kmh
from ranked
where row_num = 1
  and temperature_c between -90 and 60
  and precipitation_mm >= 0
