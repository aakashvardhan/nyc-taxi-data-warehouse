{{
    config(
        materialized='table',
        tags=['mart']
    )
}}

with trips as (
    select * from {{ ref('int_trips_enriched') }}
)

select
    -- Zone identifiers
    pickup_zone_id,
    dropoff_zone_id,
    
    -- Trip metrics
    count(*) as trip_count,
    avg(trip_distance) as avg_trip_distance,
    avg(total_amount) as avg_total_amount,
    avg(trip_duration_minutes) as avg_trip_duration_minutes,
    
    -- Passenger metrics
    avg(passenger_count) as avg_passenger_count,
    
    -- Time patterns
    sum(case when is_weekend then 1 else 0 end) as weekend_trip_count,
    sum(case when not is_weekend then 1 else 0 end) as weekday_trip_count,
    
    -- Calculate peak hour (most common hour of day for this zone pair)
    mode(hour_of_day) as peak_hour

from trips
group by 1, 2
having count(*) >= 10  -- Filter out zone pairs with very few trips
