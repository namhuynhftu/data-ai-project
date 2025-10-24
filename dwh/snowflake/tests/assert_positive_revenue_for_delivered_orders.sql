-- Test: Revenue should be positive for completed orders
select
    fp.order_id,
    sum(fp.payment_value) as total_revenue
from {{ ref('fact_payments') }} as fp
inner join {{ ref('fact_orders_accumulating') }} as fo
    on fp.order_id = fo.order_id
where fo.order_status = 'delivered'
group by fp.order_id
having sum(fp.payment_value) <= 0
