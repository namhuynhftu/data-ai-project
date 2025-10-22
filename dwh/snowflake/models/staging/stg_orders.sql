with source as (
    select * from {{ source('raw_data', 'olist_orders_dataset') }}
),

renamed as (
    select
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date,

        -- Add calculated fields
        case
            when order_delivered_customer_date is not null
                then datediff('day', order_purchase_timestamp, order_delivered_customer_date)
        end as delivery_days,

        case
            when order_delivered_customer_date is not null and order_estimated_delivery_date is not null
                then datediff('day', order_delivered_customer_date, order_estimated_delivery_date)
        end as delivery_vs_estimate_days,

        case
            when order_delivered_customer_date <= order_estimated_delivery_date then 'on_time'
            when order_delivered_customer_date > order_estimated_delivery_date then 'late'
            else 'unknown'
        end as delivery_performance,

        -- Add metadata
        current_timestamp() as _loaded_at

    from source
)

select * from renamed
