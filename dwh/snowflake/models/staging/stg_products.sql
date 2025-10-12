{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw_data', 'olist_products_dataset') }}
),

renamed as (
    select
        product_id,
        product_category_name,
        product_name_lenght,
        product_description_lenght,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm,
        
        -- Add calculated fields
        product_length_cm * product_height_cm * product_width_cm as product_volume_cm3,
        
        case 
            when product_weight_g is not null and product_weight_g > 0 then product_weight_g / 1000.0
            else null
        end as product_weight_kg,
        
        case 
            when product_photos_qty = 0 then 'no_photos'
            when product_photos_qty between 1 and 3 then 'few_photos'
            when product_photos_qty between 4 and 6 then 'moderate_photos'
            else 'many_photos'
        end as photo_category,
        
        -- Add metadata
        current_timestamp() as _loaded_at
        
    from source
)

select * from renamed