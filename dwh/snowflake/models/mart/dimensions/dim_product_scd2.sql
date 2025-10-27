{{
    config(
        materialized='incremental',
        unique_key=['product_id', 'effective_date'],
        on_schema_change='sync_all_columns',
        cluster_by=['product_id', 'is_current'],
        tags=['dimension', 'scd2', 'incremental']
    )
}}

with source_data as (
    select
        {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,
        product_id,
        product_category_name,
        product_name_length,
        product_description_length,
        product_photos_qty,
        product_volume_cm3,
        product_weight_kg,
        photo_category,
        {{ dbt.current_timestamp() }} as effective_date,
        convert_timezone('UTC', '9999-12-31 00:00:00') as end_date,
        true as is_current,
        {{ dbt_utils.generate_surrogate_key([
            'product_category_name',
            'product_name_length',
            'product_description_length',
            'product_photos_qty',
            'product_volume_cm3',
            'product_weight_kg',
            'photo_category'
        ]) }} as row_hash
    from {{ ref('int_products_cleaned') }}
),

{% if is_incremental() %}

-- Handle SCD Type 2 logic
    existing_records as (
        select * from {{ this }}
        where is_current = true
    ),

    changed_records as (
        select
            s.*,
            e.product_key as existing_key,
            e.row_hash as existing_hash
        from source_data as s
        left join existing_records as e
            on s.product_id = e.product_id
        where
            e.row_hash is null  -- New record
            or s.row_hash != e.row_hash  -- Changed record
    ),

    expired_records as (
        select
            e.product_key,
            e.product_id,
            e.product_category_name,
            e.product_name_length,
            e.product_description_length,
            e.product_photos_qty,
            e.product_volume_cm3,
            e.product_weight_kg,
            e.photo_category,
            e.effective_date,
            {{ dbt.current_timestamp() }} as end_date,
            false as is_current,
            e.row_hash
        from existing_records as e
        inner join changed_records as c
            on e.product_id = c.product_id
    ),

    final as (
        select
            product_key,
            product_id,
            product_category_name,
            product_name_length,
            product_description_length,
            product_photos_qty,
            product_volume_cm3,
            product_weight_kg,
            photo_category,
            effective_date,
            end_date,
            is_current,
            row_hash
        from changed_records
        union all
        select
            product_key,
            product_id,
            product_category_name,
            product_name_length,
            product_description_length,
            product_photos_qty,
            product_volume_cm3,
            product_weight_kg,
            photo_category,
            effective_date,
            end_date,
            is_current,
            row_hash
        from expired_records
    )

    select
        product_key,
        product_id,
        product_category_name,
        product_name_length,
        product_description_length,
        product_photos_qty,
        product_volume_cm3,
        product_weight_kg,
        photo_category,
        effective_date,
        end_date,
        is_current,
        row_hash
    from final

{% else %}

-- Initial load
select
    product_key,
    product_id,
    product_category_name,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_volume_cm3,
    product_weight_kg,
    photo_category,
    effective_date,
    end_date,
    is_current,
    row_hash
from source_data

{% endif %}
