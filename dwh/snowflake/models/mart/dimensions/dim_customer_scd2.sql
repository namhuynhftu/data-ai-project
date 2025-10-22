{{
    config(
        materialized='incremental',
        unique_key='customer_key',
        on_schema_change='sync_all_columns',
        tags=['dimension', 'scd2']
    )
}}

with source_data as (
    select
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_key,
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        geo_city,
        geo_state,
        -- SCD Type 2 fields
        {{ dbt.current_timestamp() }} as effective_date,
        cast('9999-12-31' as timestamp) as end_date,
        true as is_current,
        -- Hash for change detection
        {{ dbt.hash(dbt.concat([
            'customer_city',
            'customer_state',
            'geo_city',
            'geo_state'
        ])) }} as row_hash
    from {{ ref('int_customers_geocoded') }}
)

{% if is_incremental() %}

-- Handle SCD Type 2 logic
, existing_records as (
    select * from {{ this }}
    where is_current = true
)

, changed_records as (
    select
        s.*,
        e.customer_key as existing_key,
        e.row_hash as existing_hash
    from source_data s
    left join existing_records e
        on s.customer_id = e.customer_id
    where e.row_hash is null  -- New record
       or s.row_hash != e.row_hash  -- Changed record
)

, expired_records as (
    select
        e.customer_key,
        e.customer_id,
        e.customer_unique_id,
        e.customer_zip_code_prefix,
        e.customer_city,
        e.customer_state,
        e.geo_city,
        e.geo_state,
        e.effective_date,
        {{ dbt.current_timestamp() }} as end_date,
        false as is_current,
        e.row_hash
    from existing_records e
    inner join changed_records c
        on e.customer_id = c.customer_id
)

, final as (
    select * from changed_records
    union all
    select * from expired_records
)

select
    customer_key,
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
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
    customer_key,
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    geo_city,
    geo_state,
    effective_date,
    end_date,
    is_current,
    row_hash
from source_data

{% endif %}
