-- Test: Orders should not have delivery date before purchase date
select
    order_id,
    purchase_date,
    delivered_date
from {{ ref('fact_orders_accumulating') }}
where delivered_date < purchase_date
