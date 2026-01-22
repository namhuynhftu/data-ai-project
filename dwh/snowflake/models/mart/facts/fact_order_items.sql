{{
    config(
        materialized='incremental',
        unique_key='order_item_key',
        on_schema_change='sync_all_columns',
        incremental_strategy='merge',
        tags=['mart', 'fact', 'incremental']
    )
}}


with base as (
    select
        oi.order_id,
        oi.order_item_id,
        c.customer_id,
        s.seller_id,
        p.product_id,
        oi.purchase_date,
        oi.delivered_date,
        oi.price,
        oi.freight_value,
        oi.total_item_value
    from {{ ref('int_order_items_enriched') }} as oi
    inner join {{ ref('int_customers_geocoded') }} as c on oi.customer_id = c.customer_id
    inner join {{ ref('int_sellers_geocoded') }} as s on oi.seller_id = s.seller_id
    inner join {{ ref('int_products_cleaned') }} as p on oi.product_id = p.product_id
)

select
    {{ dbt_utils.generate_surrogate_key(['order_id','order_item_id']) }} as order_item_key,
    order_id,
    order_item_id,
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_key,
    {{ dbt_utils.generate_surrogate_key(['seller_id']) }} as seller_key,
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,
    cast(purchase_date as date) as purchase_date,
    cast(delivered_date as date) as delivered_date,
    price,
    freight_value,
    total_item_value
from base
where
    1 = 1
    {% if is_incremental() %}
        and purchase_date >= coalesce(
            (select max(purchase_date) as max_date from {{ this }}),
            '1900-01-01'
        )
    {% endif %}
