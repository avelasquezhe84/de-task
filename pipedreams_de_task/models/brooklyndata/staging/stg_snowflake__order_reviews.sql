with order_reviews as (
    select * from {{ source('brooklyndata', 'olist_order_reviews_dataset') }}
)

, final as (
    select
        trim(review_id) as review_id,
        trim(order_id) as order_id,
        try_to_number(review_score, 2, 0) as review_score,
        trim(review_comment_title) as review_comment_title,
        trim(review_comment_message) as review_comment_message,
        try_to_date(review_creation_date, 'YYYY-MM-DD HH24:MI:SS') as review_creation_date,
        try_to_date(review_answer_timestamp, 'YYYY-MM-DD HH24:MI:SS') as review_answer_timestamp
    from
        order_reviews
)

select * from final
