{{
  config(materialized = 'table')
}}

with news as (
    select * from {{ ref('stg_news') }}
    where is_recent = true
),

countries as (
    select * from {{ ref('stg_countries') }}
),

-- RestCountries only returns GB; join is static enrichment
enriched as (
    select
        n.ingested_at,
        n.article_id,
        n.title,
        n.description,
        n.url,
        n.published_at,
        n.source_name,
        n.is_recent,
        n._source,

        c.country_name,
        c.region,
        c.population
    from news n
    cross join countries c
    where c.country_code = 'GB'
)

select * from enriched
order by published_at desc
