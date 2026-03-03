{{ config(materialized='table') }}

with bounds as (
    select
        min(cast(observation_ts_utc as date)) as min_date,
        max(cast(observation_ts_utc as date)) as max_date
    from {{ ref('silver_weather_events') }}
),
series as (
    select cast(day_value as date) as date_day
    from bounds
    cross join generate_series(min_date, max_date, interval 1 day) as t(day_value)
)

select
    cast(strftime(date_day, '%Y%m%d') as integer) as date_key,
    date_day,
    cast(strftime(date_day, '%Y') as integer) as year_num,
    cast(strftime(date_day, '%m') as integer) as month_num,
    cast(strftime(date_day, '%d') as integer) as day_num,
    cast(strftime(date_day, '%w') as integer) as day_of_week_num,
    case
        when cast(strftime(date_day, '%w') as integer) in (0, 6) then 1
        else 0
    end as is_weekend
from series
