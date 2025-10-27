{{
    config(
        materialized='incremental',
        unique_key='order_id',
        on_schema_change='sync_all_columns',
        tags=['fact', 'incremental']
    )
}}

select
    r.order_id,
    r.review_score,
    r.review_sentiment,
    r.review_creation_date,
    r.review_answer_timestamp,
    r.has_comment
from {{ ref('int_reviews_cleaned') }} as r
{% if is_incremental() %}
    -- Incremental logic: only process orders modified since last run
    where
        r.review_creation_date > (select max(review_creation_date) as max_review_date from {{ this }})
{% endif %}
