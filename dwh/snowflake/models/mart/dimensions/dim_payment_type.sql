select
    {{ dbt_utils.generate_surrogate_key(['payment_type']) }} as payment_type_key,
    payment_type
from {{ ref('stg_order_payments') }}
group by payment_type
