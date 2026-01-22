{{
    config(
        materialized='view',
        tags=['staging']
    )
}}

with source as (
    select * from {{ source('raw_data', 'olist_customers_dataset') }}
),

renamed as (
    select
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,

        -- Add metadata
        current_timestamp() as _loaded_at

    from source
)

select * from renamed
