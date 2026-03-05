with source as (
    select * from {{ source('raw', 'raw_air_quality') }}
),

cleaned as (
    select
        ingested_at,
        location_city,
        cast(location_id as int64)       as location_id,
        parameter,
        cast(value       as float64)     as value,
        unit,
        cast(measured_at as timestamp)   as measured_at,
        _source
    from source
    where cast(value as float64) >= 0   -- remove sensor error readings
)

select * from cleaned
