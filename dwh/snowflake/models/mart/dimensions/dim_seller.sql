select
    {{ dbt_utils.generate_surrogate_key(['seller_id']) }} as seller_key,
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    geo_city,
    geo_state
from {{ ref('int_sellers_geocoded') }}
