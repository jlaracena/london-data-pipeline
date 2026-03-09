with source as (
    select * from {{ source('raw', 'raw_bank_holidays') }}
),

typed as (
    select
        ingested_at,
        division,
        title,
        parse_date('%Y-%m-%d', holiday_date) as holiday_date,
        nullif(trim(notes), '')              as notes,
        bunting,
        _source,

        -- Flag holidays relevant to England & Wales (where Forest operates)
        division = 'england-and-wales' as is_england_wales

    from source
)

select * from typed
