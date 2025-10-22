with joined as (
    select
        p.product_id,
        p.product_name_length,
        p.product_description_length,
        p.product_photos_qty,
        p.product_volume_cm3,
        p.product_weight_kg,
        p.photo_category,
        coalesce(t.product_category_name_english, p.product_category_name) as product_category_name
    from {{ ref('stg_products') }} as p
    left join {{ ref('stg_product_category_translation') }} as t
        on p.product_category_name = t.product_category_name
)

select * from joined
