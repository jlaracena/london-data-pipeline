with source as (
    select * from {{ source('raw', 'raw_weather') }}
),

renamed as (
    select
        ingested_at,
        location_city,
        cast(temperature_2m as float64)  as temperature_celsius,
        cast(windspeed_10m  as float64)  as windspeed_kmh,
        cast(weathercode    as int64)    as weathercode,
        cast(forecast_date  as timestamp) as forecast_at,
        _source,

        case cast(weathercode as int64)
            when 0  then 'Clear sky'
            when 1  then 'Mainly clear'
            when 2  then 'Partly cloudy'
            when 3  then 'Overcast'
            when 45 then 'Fog'
            when 61 then 'Rain'
            when 71 then 'Snow'
            when 80 then 'Rain showers'
            when 95 then 'Thunderstorm'
            else 'Unknown'
        end as weather_description

    from source
)

select * from renamed
