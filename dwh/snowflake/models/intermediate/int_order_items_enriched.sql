{{
    config(
        materialized='view',
        tags=['intermediate']
    )
}}

with joined as (
    select
        oi.order_id,
        oi.order_item_id,
        oi.product_id,
        oi.seller_id,
        o.customer_id,
        o.purchase_date,
        o.approved_date,
        o.delivered_date,
        o.estimated_date,
        oi.price,
        oi.freight_value,
        oi.total_item_value
    from {{ ref('stg_order_items') }} as oi
    inner join {{ ref('int_orders_lifecycle') }} as o
        on oi.order_id = o.order_id
)

select
    *,
    datediff(day, purchase_date, delivered_date) as days_to_deliver
from joined
