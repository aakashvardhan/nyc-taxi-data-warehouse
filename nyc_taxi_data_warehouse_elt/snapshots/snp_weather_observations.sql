{% snapshot snp_weather_observations %}
{#
    Snapshot: Weather Observations
    
    Tracks historical changes in weather conditions for NYC.
    Enables time-series analysis of weather patterns and their 
    correlation with taxi demand over time.
    
    Strategy: timestamp (uses load_ts to detect changes)
    Grain: One record per city + observation timestamp
#}

{{
    config(
        target_schema='SNAPSHOTS',
        unique_key=['city', 'observed_at'],
        strategy='timestamp',
        updated_at='load_ts',
        invalidate_hard_deletes=True
    )
}}

select
    -- Natural key components
    observed_at,
    city,
    
    -- Weather measurements
    temp_f as temperature_fahrenheit,
    weather_desc as weather_description,
    humidity_pct as humidity_percent,
    
    -- Metadata
    load_ts,
    raw_json,
    
    -- Surrogate key for downstream joins
    {{ dbt_utils.generate_surrogate_key(['city', 'observed_at']) }} as weather_observation_key

from {{ source('raw', 'RAW_WEATHER') }}
where observed_at is not null
  and city is not null

{% endsnapshot %}

