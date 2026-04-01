-- STAGING: order_reviews

with source as (
    select * from {{ source('olist', 'order_reviews') }}
),

renamed as (
    select
        review_id,
        order_id,
        cast(review_score as int)               as review_score,
        review_comment_title                    as comment_title,
        review_comment_message                  as comment_message,
        cast(review_creation_date as timestamp) as review_created_at,
        cast(review_answer_timestamp as timestamp) as review_answered_at,

        -- categorise score into sentiment buckets
        case
            when review_score >= 4 then 'positive'
            when review_score = 3  then 'neutral'
            else                        'negative'
        end as review_sentiment

    from source
    where review_id is not null
      and order_id  is not null
)

select * from renamed
