{{
    config(
        materialized='view',
        tags=['intermediate', 'taxi']
    )
}}

with trips as (
    select * from {{ ref('stg_taxi_trips') }}
),

enriched as (
    select
        -- Original fields
        pickup_datetime,
        dropoff_datetime,
        pickup_zone_id,
        dropoff_zone_id,
        passenger_count,
        trip_distance,
        total_amount,
        
        -- Calculated metrics
        datediff('minute', pickup_datetime, dropoff_datetime) as trip_duration_minutes,
        case 
            when datediff('minute', pickup_datetime, dropoff_datetime) > 0 
            then (trip_distance / (datediff('minute', pickup_datetime, dropoff_datetime) / 60.0))
            else 0 
        end as avg_speed_mph,
        
        -- Time-based features
        date_trunc('hour', pickup_datetime) as pickup_hour,
        extract(hour from pickup_datetime) as hour_of_day,
        dayofweek(pickup_datetime) as day_of_week,
        dayname(pickup_datetime) as day_name,
        case when dayofweek(pickup_datetime) in (0, 6) then true else false end as is_weekend,
        
        -- Anomaly flags
        case 
            when datediff('minute', pickup_datetime, dropoff_datetime) < 1 then true
            when datediff('minute', pickup_datetime, dropoff_datetime) > 180 then true
            else false 
        end as is_trip_duration_anomaly,
        
        case 
            when trip_distance < 0.1 then true
            when trip_distance > 100 then true
            else false 
        end as is_trip_distance_anomaly,
        
        case 
            when passenger_count = 0 or passenger_count is null then true
            when passenger_count > 6 then true
            else false 
        end as is_passenger_count_anomaly,
        
        -- Metadata
        load_ts
        
    from trips
)

select * from enriched
