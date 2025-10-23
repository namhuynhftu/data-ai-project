-- Test: Revenue should be positive for completed orders
select
    order_id,
    order_status,
    sum(payment_value) as total_revenue
from {{ ref('fact_payments') }} as f
join {{ ref('dim_payment_type')}} as pt
    on f.payment_type_key = pt.payment_type_key
where order_status = 'delivered'
group by order_id, order_status
having sum(payment_value) <= 0
