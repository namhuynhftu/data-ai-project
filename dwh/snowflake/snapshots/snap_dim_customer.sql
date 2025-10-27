{% snapshot snap_dim_customer %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='check',
        check_cols=[
            'customer_city',
            'customer_state',
            'geo_city',
            'geo_state'
        ],
        invalidate_hard_deletes=True,
        tags=['snapshot', 'scd2', 'customer']
    )
}}

select
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_key,
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    geo_city,
    geo_state
from {{ ref('int_customers_geocoded') }}

{% endsnapshot %}
