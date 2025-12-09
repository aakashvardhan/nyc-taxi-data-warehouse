{% snapshot snp_daily_metrics %}
{#
    Snapshot: Daily Trip Metrics
    
    Tracks changes in daily aggregated metrics over time.
    Useful for auditing when late-arriving taxi data changes 
    historical totals and for trend analysis.
    
    Strategy: check (monitors specific columns for changes)
    Grain: One record per trip_date
#}

{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='trip_date',
        strategy='check',
        check_cols=[
            'trip_count',
            'total_revenue',
            'avg_fare',
            'avg_distance',
            'weekend_trip_count',
            'weekday_trip_count'
        ]
    )
}}

select
    -- Primary key
    trip_date,
    
    -- Trip volume metrics
    trip_count,
    weekend_trip_count,
    weekday_trip_count,
    
    -- Revenue metrics
    total_revenue,
    avg_fare,
    
    -- Trip characteristics
    avg_distance,
    avg_duration_minutes,
    avg_passenger_count,
    
    -- Weather context (for reference)
    avg_daily_temperature,
    min_daily_temperature,
    max_daily_temperature,
    avg_daily_humidity,
    
    -- Snapshot metadata
    current_timestamp() as snapshot_captured_at

from {{ ref('mart_daily_metrics') }}
where trip_date is not null

{% endsnapshot %}

