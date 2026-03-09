{{
  config(
    materialized = 'table',
    partition_by = {
      'field': 'month_date',
      'data_type': 'date'
    }
  )
}}

-- Daily safety and bike availability overview for London.
-- Combines crime statistics with Santander Cycles dock availability —
-- useful for Forest eBikes to identify zones where safety conditions
-- may affect micro-mobility demand.

with crime as (
    select * from {{ ref('stg_crime') }}
),

bikepoints as (
    select * from {{ ref('stg_tfl_bikepoints') }}
),

bank_holidays as (
    select * from {{ ref('stg_bank_holidays') }}
    where is_england_wales = true
),

-- Crime aggregated by month and category
monthly_crime as (
    select
        month_date,
        category,
        count(*)                                    as incident_count,
        countif(is_resolved)                        as resolved_count,
        countif(is_bicycle_theft)                   as bicycle_theft_count,
        round(
            safe_divide(countif(is_resolved), count(*)) * 100, 1
        )                                           as resolution_rate_pct
    from crime
    group by 1, 2
),

-- Monthly bike availability snapshot (aggregated from daily data to match crime granularity)
daily_bikes as (
    select
        date_trunc(snapshot_date, month)            as month_date,
        count(distinct station_id)                  as total_stations,
        round(avg(nb_bikes), 0)                     as total_bikes_available,
        round(avg(nb_docks), 0)                     as total_capacity,
        round(avg(occupancy_rate) * 100, 1)         as avg_occupancy_pct
    from bikepoints
    group by 1
)

select
    c.month_date,
    c.category                                      as crime_category,
    c.incident_count,
    c.resolved_count,
    c.bicycle_theft_count,
    c.resolution_rate_pct,
    b.total_stations,
    b.total_bikes_available,
    b.total_capacity,
    b.avg_occupancy_pct,

    -- Flag if this month contains a bank holiday (affects ridership)
    exists (
        select 1 from bank_holidays h
        where date_trunc(h.holiday_date, month) = c.month_date
    )                                               as has_bank_holiday

from monthly_crime c
left join daily_bikes b
    on b.month_date = c.month_date
