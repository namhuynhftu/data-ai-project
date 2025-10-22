with geo as (
    select
        geolocation_zip_code_prefix,
        any_value(geolocation_city) as city,
        any_value(geolocation_state) as state
    from {{ ref('stg_geolocation') }}
    group by geolocation_zip_code_prefix
)
select
    {{ dbt_utils.generate_surrogate_key(['geolocation_zip_code_prefix']) }} as geo_key,
    geolocation_zip_code_prefix,
    city,
    state
from geo
