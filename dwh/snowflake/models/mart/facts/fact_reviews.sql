select
    r.order_id,
    r.review_score,
    r.review_sentiment,
    r.review_creation_date,
    r.review_answer_timestamp,
    r.has_comment
from {{ ref('int_reviews_cleaned') }} as r