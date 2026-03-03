{{ config(materialized='table') }}

select
    row_number() over (order by location_name, latitude, longitude) as location_key,
    md5(location_name || '|' || cast(latitude as varchar) || '|' || cast(longitude as varchar)) as location_nk,
    location_name,
    latitude,
    longitude,
    timezone
from {{ ref('silver_locations') }}
