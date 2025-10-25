{{
    config(
        materialized='table',
        cluster_by=['order_month'],
        tags=['metrics', 'business']
    )
}}

-- Sales Metrics Mart
-- Aggregated business metrics for executive reporting and analytics

with order_items as (
    select
        fi.purchase_date,
        fi.customer_key,
        fi.seller_key,
        fi.product_key,
        fi.price,
        fi.freight_value,
        fi.total_item_value,
        dc.customer_state,
        ds.seller_state,
        dp.product_category_name
    from {{ ref('fact_order_items') }} as fi
    inner join {{ ref('dim_customer') }} as dc on fi.customer_key = dc.customer_key
    inner join {{ ref('dim_seller') }} as ds on fi.seller_key = ds.seller_key
    inner join {{ ref('dim_product') }} as dp on fi.product_key = dp.product_key
),

orders as (
    select
        order_id,
        customer_id,
        order_status,
        purchase_date,
        on_time_flag,
        purchase_to_approve_days,
        ship_to_deliver_days
    from {{ ref('fact_orders_accumulating') }}
),

reviews as (
    select
        order_id,
        review_score
    from {{ ref('fact_reviews') }}
)

select
    date_trunc('month', oi.purchase_date) as order_month,
    oi.customer_state,
    oi.seller_state,
    oi.product_category_name,

    -- Volume Metrics
    count(distinct o.order_id) as total_orders,
    count(distinct oi.customer_key) as unique_customers,
    count(distinct oi.seller_key) as unique_sellers,
    count(*) as total_items_sold,

    -- Revenue Metrics
    sum(oi.price) as gross_merchandise_value,
    sum(oi.freight_value) as total_freight_revenue,
    sum(oi.total_item_value) as total_revenue,
    avg(oi.price) as avg_item_price,
    avg(oi.total_item_value) as avg_order_value,

    -- Operational Metrics
    avg(o.purchase_to_approve_days) as avg_approval_time_days,
    avg(o.ship_to_deliver_days) as avg_delivery_time_days,
    sum(o.on_time_flag) / count(o.order_id) as on_time_delivery_rate,

    -- Customer Satisfaction Metrics
    avg(r.review_score) as avg_review_score,
    count(case when r.review_score >= 4 then 1 end) / count(r.review_score) as positive_review_rate

from order_items as oi
inner join orders as o
    on
        oi.customer_key = {{ dbt_utils.generate_surrogate_key(['o.customer_id']) }}
        and oi.purchase_date = o.purchase_date
left join reviews as r on o.order_id = r.order_id

group by 1, 2, 3, 4
