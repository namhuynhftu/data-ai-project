{{
    config(
        materialized='table',
        cluster_by=['customer_key'],
        tags=['metrics', 'customer']
    )
}}

-- Customer Metrics Mart
-- Customer lifetime value, segmentation, and behavioral metrics

with customer_orders as (
    select
        fi.customer_key,
        min(fi.purchase_date) as first_order_date,
        max(fi.purchase_date) as last_order_date,
        count(distinct fi.order_id) as total_orders,
        count(*) as total_items_purchased,
        sum(fi.total_item_value) as lifetime_value,
        avg(fi.total_item_value) as avg_order_value
    from {{ ref('fact_order_items') }} fi
    group by 1
),

customer_reviews as (
    select
        {{ dbt_utils.generate_surrogate_key(['o.customer_id']) }} as customer_key,
        avg(r.review_score) as avg_review_score,
        count(r.order_id) as total_reviews
    from {{ ref('fact_reviews') }} r
    inner join {{ ref('fact_orders_accumulating') }} o on r.order_id = o.order_id
    group by 1
),

customer_dim as (
    select
        customer_key,
        customer_id,
        customer_state,
        customer_city
    from {{ ref('dim_customer') }}
)

select
    cd.customer_key,
    cd.customer_id,
    cd.customer_state,
    cd.customer_city,
    
    -- Order Behavior Metrics
    co.first_order_date,
    co.last_order_date,
    datediff('day', co.first_order_date, co.last_order_date) as customer_tenure_days,
    co.total_orders,
    co.total_items_purchased,
    
    -- Value Metrics
    co.lifetime_value as customer_lifetime_value,
    co.avg_order_value,
    co.lifetime_value / nullif(co.total_orders, 0) as revenue_per_order,
    
    -- Engagement Metrics
    cr.total_reviews,
    cr.avg_review_score,
    
    -- Customer Segmentation
    {{ calculate_customer_segment('co.total_orders', 'co.lifetime_value') }} as customer_segment,
    
    -- Recency, Frequency, Monetary (RFM)
    datediff('day', co.last_order_date, current_date()) as recency_days,
    co.total_orders as frequency,
    co.lifetime_value as monetary_value

from customer_dim cd
inner join customer_orders co on cd.customer_key = co.customer_key
left join customer_reviews cr on cd.customer_key = cr.customer_key
