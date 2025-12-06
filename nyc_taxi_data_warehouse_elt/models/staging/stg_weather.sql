{{
    config(
        materialized='view',
        tags=['staging', 'weather']
    )
}}

with source as (
    select * from {{ source('raw', 'RAW_WEATHER') }}
),

renamed as (
    select
        -- Timestamps
        observed_at,
        load_ts,
        
        -- Location
        city,
        
        -- Weather metrics
        temp_f as temperature_fahrenheit,
        weather_desc as weather_description,
        humidity_pct as humidity_percent,
        
        -- Raw JSON for reference
        raw_json
        
    from source
    where 
        -- Data quality filters
        observed_at is not null
        and temp_f is not null
)

select * from renamed
