-- Test: Revenue should be positive for completed orders
select
    order_id,
    order_status,
    sum(payment_value) as total_revenue
from {{ ref('fact_payments') }}
where order_status = 'delivered'
group by order_id, order_status
having sum(payment_value) <= 0
