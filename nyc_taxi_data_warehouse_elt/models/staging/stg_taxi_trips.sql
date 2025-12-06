{{
    config(
        materialized='view',
        tags=['staging', 'taxi']
    )
}}

with source as (
    select * from {{ source('raw', 'NYC_TAXI_TRIPS') }}
),

renamed as (
    select
        -- Primary timestamp
        pickup_datetime,
        dropoff_datetime,
        
        -- Location identifiers
        pickup_zone_id,
        dropoff_zone_id,
        
        -- Trip details
        passenger_count,
        trip_distance,
        total_amount,
        
        -- Metadata
        load_ts
        
    from source
    where 
        -- Data quality filters
        pickup_datetime is not null
        and dropoff_datetime is not null
        and pickup_zone_id is not null
        and dropoff_zone_id is not null
        and trip_distance > 0
        and total_amount > 0
        and dropoff_datetime > pickup_datetime  -- Ensure valid trip duration
)

select * from renamed
