{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw_data', 'product_category_name_translation') }}
),

renamed as (
    select
        product_category_name,
        product_category_name_english,
        
        -- Add metadata
        current_timestamp() as _loaded_at
        
    from source
)

select * from renamed