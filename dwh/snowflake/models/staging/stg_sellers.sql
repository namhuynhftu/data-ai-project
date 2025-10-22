with source as (
    select * from {{ source('raw_data', 'olist_sellers_dataset') }}
),

renamed as (
    select
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state,

        -- Add metadata
        current_timestamp() as _loaded_at

    from source
)

select * from renamed
