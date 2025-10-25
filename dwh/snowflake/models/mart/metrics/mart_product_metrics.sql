{{
    config(
        materialized='table',
        cluster_by=['product_category_name'],
        tags=['metrics', 'product']
    )
}}

-- Product Metrics Mart
-- Product performance, inventory insights, and category analytics

with product_sales as (
    select
        fi.product_key,
        count(distinct fi.order_id) as orders_containing_product,
        count(*) as total_units_sold,
        sum(fi.price) as total_sales_revenue,
        sum(fi.freight_value) as total_freight_charged,
        avg(fi.price) as avg_selling_price,
        min(fi.purchase_date) as first_sale_date,
        max(fi.purchase_date) as last_sale_date
    from {{ ref('fact_order_items') }} as fi
    group by 1
),

product_reviews as (
    select
        fi.product_key,
        avg(r.review_score) as avg_product_rating,
        count(r.order_id) as total_reviews,
        count(case when r.review_score >= 4 then 1 end) as positive_reviews
    from {{ ref('fact_order_items') }} as fi
    inner join {{ ref('fact_reviews') }} as r on fi.order_id = r.order_id
    group by 1
),

product_dim as (
    select
        product_key,
        product_id,
        product_category_name,
        product_weight_kg,
        product_volume_cm3,
        product_photos_qty
    from {{ ref('dim_product') }}
)

select
    pd.product_key,
    pd.product_id,
    pd.product_category_name,
    pd.product_weight_kg,
    pd.product_volume_cm3,
    pd.product_photos_qty,

    -- Sales Performance Metrics
    ps.orders_containing_product,
    ps.total_units_sold,
    ps.total_sales_revenue,
    ps.avg_selling_price,
    ps.total_freight_charged,

    -- Product Lifecycle
    ps.first_sale_date,
    ps.last_sale_date,
    pr.avg_product_rating,
    pr.total_reviews,

    -- Customer Satisfaction
    pr.positive_reviews,
    datediff('day', ps.first_sale_date, ps.last_sale_date) as product_lifespan_days,
    datediff('day', ps.last_sale_date, current_date()) as days_since_last_sale,
    pr.positive_reviews / nullif(pr.total_reviews, 0) as positive_review_rate,

    -- Business Classifications
    case
        when ps.total_units_sold >= 100 then 'Top Seller'
        when ps.total_units_sold >= 50 then 'Popular'
        when ps.total_units_sold >= 10 then 'Moderate'
        else 'Low Volume'
    end as sales_tier,

    case
        when pr.avg_product_rating >= 4.5 then 'Excellent'
        when pr.avg_product_rating >= 4.0 then 'Good'
        when pr.avg_product_rating >= 3.0 then 'Average'
        else 'Below Average'
    end as quality_tier

from product_dim as pd
inner join product_sales as ps on pd.product_key = ps.product_key
left join product_reviews as pr on pd.product_key = pr.product_key
