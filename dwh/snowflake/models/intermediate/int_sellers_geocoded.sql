{{
    config(
        materialized='view',
        tags=['intermediate']
    )
}}

with geo_deduplicated as (
    select
        geolocation_zip_code_prefix,
        any_value(geolocation_city) as geo_city,
        any_value(geolocation_state) as geo_state
    from {{ ref('stg_geolocation') }}
    group by geolocation_zip_code_prefix
),

base as (
    select
        s.seller_id,
        s.seller_zip_code_prefix,
        s.seller_city,
        s.seller_state,
        g.geo_city,
        g.geo_state
    from {{ ref('stg_sellers') }} as s
    left join geo_deduplicated as g
        on s.seller_zip_code_prefix = g.geolocation_zip_code_prefix
)

select * from base
