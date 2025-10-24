{{
    config(
        materialized='view',
        tags=['intermediate']
    )
}}

select
    order_id,
    customer_id,
    order_status,
    cast(order_purchase_timestamp as timestampntz) as purchase_date,
    cast(order_approved_at as timestampntz) as approved_date,
    cast(order_delivered_carrier_date as timestampntz) as carrier_date,
    cast(order_delivered_customer_date as timestampntz) as delivered_date,
    cast(order_estimated_delivery_date as timestampntz) as estimated_date,
    delivery_days,
    delivery_vs_estimate_days,
    delivery_performance
from {{ ref('stg_orders') }}
