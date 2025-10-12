{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw_data', 'olist_order_reviews_dataset') }}
),

renamed as (
    select
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date,
        review_answer_timestamp,
        
        -- Add calculated fields
        case 
            when review_score >= 4 then 'positive'
            when review_score = 3 then 'neutral'
            else 'negative'
        end as review_sentiment,
        
        case 
            when review_comment_message is not null then true
            else false
        end as has_comment,
        
        -- Add metadata
        current_timestamp() as _loaded_at
        
    from source
)

select * from renamed