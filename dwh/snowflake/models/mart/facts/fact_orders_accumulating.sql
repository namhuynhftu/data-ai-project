{{
    config(
        materialized='incremental',
        unique_key='order_id',
        on_schema_change='sync_all_columns',
        cluster_by=['purchase_date', 'order_status'],
        tags=['fact', 'incremental']
    )
}}

with source_data as (
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
        case when delivered_date <= estimated_date then 1 else 0 end as on_time_flag,
        {{ dbt.current_timestamp() }} as processed_at
    from {{ ref('int_orders_lifecycle') }}

    {% if is_incremental() %}
        -- Incremental logic: only process orders modified since last run
        where
            purchase_date > (select max(purchase_date) from {{ this }})
            or order_id in (
                select order_id
                from {{ ref('int_orders_lifecycle') }}
                where purchase_date >= dateadd('day', -7, current_date())
            )
    {% endif %}
)

select * from source_data
