{{
    config(
        materialized='incremental',
        unique_key='review_id',
        on_schema_change='sync_all_columns',
        incremental_strategy='merge',
        tags=['mart', 'fact', 'incremental']
    )
}}

select
    r.review_key,
    r.review_id,
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
        r.review_creation_date > dateadd(day, -7, (select max(review_creation_date) from {{ this }}))
{% endif %}
