with source as (
    select * from {{ source('raw', 'raw_countries') }}
),

renamed as (
    select
        ingested_at,
        country_code,
        country_name,
        capital,
        cast(population as int64)   as population,
        cast(area_km2   as float64) as area_km2,
        region,
        languages,
        currencies,
        _source
    from source
)

select * from renamed
