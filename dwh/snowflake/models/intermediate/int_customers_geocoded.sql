with base as (
    select
        c.customer_id,
        c.customer_unique_id,
        c.customer_zip_code_prefix,
        c.customer_city,
        c.customer_state,
        g.geolocation_city as geo_city,
        g.geolocation_state as geo_state
    from {{ ref('stg_customers') }} as c
    left join {{ ref('stg_geolocation') }} as g
        on c.customer_zip_code_prefix = g.geolocation_zip_code_prefix
)

select distinct * from base
