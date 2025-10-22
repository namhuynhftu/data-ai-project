select
    order_id,
    customer_id,
    order_status,
    purchase_date,
    approved_date,
    carrier_date,
    delivered_date,
    estimated_date,
    datediff(day, purchase_date, approved_date) as purchase_to_approve_days,
    datediff(day, approved_date, carrier_date) as approve_to_ship_days,
    datediff(day, carrier_date, delivered_date) as ship_to_deliver_days,
    case when delivered_date <= estimated_date then 1 else 0 end as on_time_flag
from {{ ref('int_orders_lifecycle') }}
