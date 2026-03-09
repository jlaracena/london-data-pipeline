{{
  config(
    materialized = 'table',
    partition_by = {
      'field': 'date',
      'data_type': 'date'
    }
  )
}}

with weather as (
    select * from {{ ref('stg_weather') }}
),

air_quality as (
    select * from {{ ref('stg_air_quality') }}
),

daily_weather as (
    select
        date(forecast_at)               as date,
        avg(temperature_celsius)        as avg_temperature_celsius,
        max(windspeed_kmh)              as max_windspeed_kmh
    from weather
    group by 1
),

daily_air_quality as (
    select
        date(measured_at)                       as date,
        array_agg(distinct parameter)           as parameters_measured
    from air_quality
    group by 1
)

select
    w.date,
    round(w.avg_temperature_celsius, 2)  as avg_temperature_celsius,
    round(w.max_windspeed_kmh, 2)        as max_windspeed_kmh,
    coalesce(aq.parameters_measured, []) as parameters_measured
from daily_weather w
left join daily_air_quality aq using (date)
