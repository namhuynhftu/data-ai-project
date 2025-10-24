{{
    config(
        materialized='table',
        cluster_by=['product_category_name'],
        tags=['dimension', 'core']
    )
}}

select
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,
    product_id,
    product_category_name,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_volume_cm3,
    product_weight_kg,
    photo_category
from {{ ref('int_products_cleaned') }}
