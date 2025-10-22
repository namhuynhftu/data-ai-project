with source as (
    select * from {{ source('raw_data', 'olist_geolocation_dataset') }}
),

renamed as (
    select
        geolocation_zip_code_prefix,
        geolocation_lat,
        geolocation_lng,
        geolocation_city,
        geolocation_state,

        -- Add metadata
        current_timestamp() as _loaded_at

    from source
)

select * from renamed
