with cleaned as (
    select
        order_id,
        review_score,
        review_sentiment,
        review_creation_date,
        review_answer_timestamp,
        has_comment
    from {{ ref('stg_order_reviews') }}
)

select * from cleaned
