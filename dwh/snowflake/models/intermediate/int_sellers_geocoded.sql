with base as (
    select
        s.seller_id,
        s.seller_zip_code_prefix,
        s.seller_city,
        s.seller_state,
        g.geolocation_city as geo_city,
        g.geolocation_state as geo_state
    from {{ ref('stg_sellers') }} as s
    left join {{ ref('stg_geolocation') }} as g
        on s.seller_zip_code_prefix = g.geolocation_zip_code_prefix
)

select distinct * from base
