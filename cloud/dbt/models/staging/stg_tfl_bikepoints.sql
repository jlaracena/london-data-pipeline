with source as (
    select * from {{ source('raw', 'raw_tfl_bikepoints') }}
),

enriched as (
    select
        ingested_at,
        snapshot_date,
        station_id,
        station_name,
        cast(lat as float64)           as lat,
        cast(lon as float64)           as lon,
        cast(nb_bikes as int64)        as nb_bikes,
        cast(nb_empty_docks as int64)  as nb_empty_docks,
        cast(nb_docks as int64)        as nb_docks,
        _source,

        -- Occupancy rate: proportion of docks currently filled with bikes
        safe_divide(
            cast(nb_bikes as float64),
            cast(nb_docks as float64)
        ) as occupancy_rate

    from source
    where nb_docks > 0  -- exclude stations with no dock data
)

select * from enriched
