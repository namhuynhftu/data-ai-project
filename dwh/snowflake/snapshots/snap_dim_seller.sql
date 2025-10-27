{% snapshot snap_dim_seller %}

{{
    config(
        target_schema='snapshots',
        unique_key='seller_id',
        strategy='check',
        check_cols=[
            'seller_city',
            'seller_state',
            'geo_city',
            'geo_state'
        ],
        invalidate_hard_deletes=True,
        tags=['snapshot', 'scd2', 'seller']
    )
}}

select
    {{ dbt_utils.generate_surrogate_key(['seller_id']) }} as seller_key,
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    geo_city,
    geo_state
from {{ ref('int_sellers_geocoded') }}

{% endsnapshot %}
