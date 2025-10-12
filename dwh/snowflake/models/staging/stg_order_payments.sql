{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw_data', 'olist_order_payments_dataset') }}
),

renamed as (
    select
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value,
        
        -- Add calculated fields
        case 
            when payment_installments > 1 then payment_value / payment_installments
            else payment_value
        end as installment_value,
        
        -- Add metadata
        current_timestamp() as _loaded_at
        
    from source
)

select * from renamed