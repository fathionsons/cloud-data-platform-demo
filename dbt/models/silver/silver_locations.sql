{{ config(materialized='table') }}

select distinct
    location_name,
    latitude,
    longitude,
    timezone
from {{ ref('silver_weather_events') }}
