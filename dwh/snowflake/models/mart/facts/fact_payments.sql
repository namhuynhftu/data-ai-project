select
    p.order_id,
    p.payment_sequential,
    {{ dbt_utils.generate_surrogate_key(['p.payment_type']) }} as payment_type_key,
    p.payment_value,
    p.payment_installments,
    p.installment_value
from {{ ref('stg_order_payments') }} as p
