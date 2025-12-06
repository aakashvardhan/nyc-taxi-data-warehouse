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
),

daily_trips as (
    select
        date_trunc('day', pickup_datetime) as trip_date,
        count(*) as trip_count,
        sum(total_amount) as total_revenue,
        avg(total_amount) as avg_fare,
        avg(trip_distance) as avg_distance,
        avg(trip_duration_minutes) as avg_duration_minutes,
        avg(passenger_count) as avg_passenger_count,
        sum(case when is_weekend then 1 else 0 end) as weekend_trip_count,
        sum(case when not is_weekend then 1 else 0 end) as weekday_trip_count
    from trips
    group by 1
),

daily_weather as (
    select
        date_trunc('day', observation_hour) as weather_date,
        avg(avg_temperature_fahrenheit) as avg_daily_temperature,
        min(min_temperature_fahrenheit) as min_daily_temperature,
        max(max_temperature_fahrenheit) as max_daily_temperature,
        avg(avg_humidity_percent) as avg_daily_humidity
    from weather
    where city = 'New York'
    group by 1
)

select
    dt.trip_date,
    dt.trip_count,
    dt.total_revenue,
    dt.avg_fare,
    dt.avg_distance,
    dt.avg_duration_minutes,
    dt.avg_passenger_count,
    dt.weekend_trip_count,
    dt.weekday_trip_count,
    
    -- Weather metrics
    dw.avg_daily_temperature,
    dw.min_daily_temperature,
    dw.max_daily_temperature,
    dw.avg_daily_humidity

from daily_trips dt
left join daily_weather dw
    on dw.weather_date = dt.trip_date
