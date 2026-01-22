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

customers_deduplicated as (
    select
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        any_value(customer_city) as customer_city,
        any_value(customer_state) as customer_state
    from {{ ref('stg_customers') }}
    group by customer_id, customer_unique_id, customer_zip_code_prefix
),

base as (
    select
        c.customer_id,
        c.customer_unique_id,
        c.customer_zip_code_prefix,
        c.customer_city,
        c.customer_state,
        g.geo_city,
        g.geo_state
    from customers_deduplicated as c
    left join geo_deduplicated as g
        on c.customer_zip_code_prefix = g.geolocation_zip_code_prefix
)

select * from base
