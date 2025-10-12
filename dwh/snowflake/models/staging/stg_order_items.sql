{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw_data', 'olist_order_items_dataset') }}
),

renamed as (
    select
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date,
        price,
        freight_value,
        
        -- Add calculated fields
        price + freight_value as total_item_value,
        
        -- Add metadata
        current_timestamp() as _loaded_at
        
    from source
)

select * from renamed