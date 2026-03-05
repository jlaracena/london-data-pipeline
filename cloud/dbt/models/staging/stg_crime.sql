with source as (
    select * from {{ source('raw', 'raw_crime') }}
),

typed as (
    select
        ingested_at,
        crime_id,
        persistent_id,
        category,
        location_type,
        cast(lat as float64) as lat,
        cast(lon as float64) as lon,
        street_name,
        outcome_status,
        month,
        parse_date('%Y-%m', month) as month_date,
        _source,

        -- Useful derived flags
        outcome_status is not null                   as is_resolved,
        category = 'bicycle-theft'                   as is_bicycle_theft

    from source
)

select * from typed
