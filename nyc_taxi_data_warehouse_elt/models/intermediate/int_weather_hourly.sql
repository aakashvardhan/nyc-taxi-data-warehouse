{{
    config(
        materialized='view',
        tags=['intermediate', 'weather']
    )
}}

with weather as (
    select * from {{ ref('stg_weather') }}
),

hourly_aggregated as (
    select
        date_trunc('hour', observed_at) as observation_hour,
        city,
        
        -- Aggregate weather metrics (in case of multiple observations per hour)
        avg(temperature_fahrenheit) as avg_temperature_fahrenheit,
        min(temperature_fahrenheit) as min_temperature_fahrenheit,
        max(temperature_fahrenheit) as max_temperature_fahrenheit,
        
        avg(humidity_percent) as avg_humidity_percent,
        
        -- Take the most recent weather description for the hour
        max_by(weather_description, observed_at) as weather_description,
        
        -- Count observations per hour for data quality
        count(*) as observation_count,
        
        -- Latest observation timestamp
        max(observed_at) as latest_observation_at
        
    from weather
    group by 1, 2
)

select * from hourly_aggregated
