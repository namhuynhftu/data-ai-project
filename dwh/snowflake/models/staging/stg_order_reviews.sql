{{
    config(
        materialized='incremental',
        unique_key='review_id',
        on_schema_change='sync_all_columns',
        incremental_strategy='merge',
        tags=['staging']
    )
}}

with source as (
    select * from {{ source('raw_data', 'olist_order_reviews_dataset') }}
    {% if is_incremental() %}
    -- Only fetch new/updated reviews from raw layer
    where review_creation_date >= dateadd(day, -7, (select max(review_creation_date) from {{ this }}))
    {% endif %}
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['review_id', 'order_id']) }} as id,
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

        coalesce(review_comment_message is not null, false) as has_comment,

        -- Add metadata
        current_timestamp() as _loaded_at

    from source
)

select * from renamed
