{% snapshot snap_dim_product %}

{{
    config(
        target_schema='snapshots',
        unique_key='product_id',
        strategy='check',
        check_cols=[
            'product_category_name',
            'product_name_length',
            'product_description_length',
            'product_photos_qty',
            'product_volume_cm3',
            'product_weight_kg',
            'photo_category'
        ],
        invalidate_hard_deletes=True,
        tags=['snapshot', 'scd2', 'product']
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

{% endsnapshot %}
