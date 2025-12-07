{{
    config(
        materialized='table',
        tags=['mart']
    )
}}

with trips as (
    select * from {{ ref('int_trips_enriched') }}
),
weather as (
    select * from {{ ref('int_weather_hourly') }}
)

select
    t.pickup_hour as hour_ts,
    
    -- Trip metrics
    count(*) as trip_count,
    avg(t.trip_distance) as avg_trip_distance,
    avg(t.total_amount) as avg_total_amount,
    avg(t.trip_duration_minutes) as avg_trip_duration_minutes,
    avg(t.avg_speed_mph) as avg_speed_mph,

    -- Weather metrics
    w.avg_temperature_fahrenheit,
    w.avg_humidity_percent,
    w.weather_description,

    -- Additional dimensions
    t.hour_of_day,
    t.day_of_week,
    t.day_name,
    t.is_weekend

from trips t
left join weather w
    on w.observation_hour = t.pickup_hour
    and lower(w.city) like '%new york%'

group by 
    t.pickup_hour,
    w.avg_temperature_fahrenheit,
    w.avg_humidity_percent,
    w.weather_description,
    t.hour_of_day,
    t.day_of_week,
    t.day_name,
    t.is_weekend
