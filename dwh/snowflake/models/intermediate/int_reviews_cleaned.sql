{{
    config(
        materialized='view',
        tags=['intermediate']
    )
}}

with cleaned as (
    select
        {{ dbt_utils.generate_surrogate_key(['order_id', 'review_id']) }} as review_key,
        review_id,
        order_id,
        review_score,
        review_sentiment,
        review_creation_date,
        review_answer_timestamp,
        has_comment
    from {{ ref('stg_order_reviews') }}
)

select * from cleaned
