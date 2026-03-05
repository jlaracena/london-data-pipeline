with source as (
    select * from {{ source('raw', 'raw_news') }}
),

enriched as (
    select
        ingested_at,
        article_id,
        title,
        description,
        url,
        cast(published_at as timestamp) as published_at,
        source_name,
        _source,

        cast(published_at as timestamp) >= timestamp_sub(current_timestamp(), interval 7 day)
            as is_recent

    from source
)

select * from enriched
