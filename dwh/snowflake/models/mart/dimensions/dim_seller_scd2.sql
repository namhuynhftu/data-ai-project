{{
    config(
        materialized='incremental',
        unique_key=['seller_id', 'effective_date'],
        on_schema_change='sync_all_columns',
        cluster_by=['seller_id', 'is_current'],
        tags=['dimension', 'scd2', 'incremental']
    )
}}

with source_data as (
    select
        {{ dbt_utils.generate_surrogate_key(['seller_id']) }} as seller_key,
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state,
        geo_city,
        geo_state,
        -- SCD Type 2 fields
        {{ dbt.current_timestamp() }} as effective_date,
        convert_timezone('UTC', '9999-12-31 00:00:00') as end_date,
        true as is_current,
        -- Hash for change detection
        {{ dbt_utils.generate_surrogate_key([
            'seller_city',
            'seller_state',
            'geo_city',
            'geo_state'
        ]) }} as row_hash
    from {{ ref('int_sellers_geocoded') }}
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
            e.seller_key as existing_key,
            e.row_hash as existing_hash
        from source_data as s
        left join existing_records as e
            on s.seller_id = e.seller_id
        where
            e.row_hash is null  -- New record
            or s.row_hash != e.row_hash  -- Changed record
    ),

    expired_records as (
        select
            e.seller_key,
            e.seller_id,
            e.seller_zip_code_prefix,
            e.seller_city,
            e.seller_state,
            e.geo_city,
            e.geo_state,
            e.effective_date,
            {{ dbt.current_timestamp() }} as end_date,
            false as is_current,
            e.row_hash
        from existing_records as e
        inner join changed_records as c
            on e.seller_id = c.seller_id
    ),

    final as (
        select
            seller_key,
            seller_id,
            seller_zip_code_prefix,
            seller_city,
            seller_state,
            geo_city,
            geo_state,
            effective_date,
            end_date,
            is_current,
            row_hash
        from changed_records
        union all
        select
            seller_key,
            seller_id,
            seller_zip_code_prefix,
            seller_city,
            seller_state,
            geo_city,
            geo_state,
            effective_date,
            end_date,
            is_current,
            row_hash
        from expired_records
    )

    select
        seller_key,
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state,
        geo_city,
        geo_state,
        effective_date,
        end_date,
        is_current,
        row_hash
    from final

{% else %}

-- Initial load
select
    seller_key,
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    geo_city,
    geo_state,
    effective_date,
    convert_timezone('UTC', '9999-12-31 00:00:00') as end_date,
    is_current,
    row_hash
from source_data

{% endif %}
