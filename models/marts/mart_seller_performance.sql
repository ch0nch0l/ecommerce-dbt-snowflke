-- MART: mart_seller_performance
-- ─────────────────────────────────────────────────────────────────────────────
-- Purpose : Seller-level performance metrics — revenue, delivery, satisfaction.
--           Answers: which sellers are our best performers? Which need support?
-- Grain   : One row per seller
-- ─────────────────────────────────────────────────────────────────────────────

with sellers as (
    select * from {{ ref('stg_sellers') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select
        order_id,
        review_score,
        review_sentiment,
        days_to_deliver,
        is_on_time_delivery,
        order_status
    from {{ ref('fct_orders') }}
    where order_status not in ('canceled', 'unavailable')
),

seller_metrics as (
    select
        oi.seller_id,
        count(distinct oi.order_id)                     as total_orders,
        count(*)                                        as total_items_sold,
        count(distinct oi.product_id)                   as unique_products,
        round(sum(oi.price),                2)          as total_revenue,
        round(avg(oi.price),                2)          as avg_item_price,
        round(avg(o.review_score),          2)          as avg_review_score,
        round(avg(o.days_to_deliver),       1)          as avg_days_to_deliver,
        round(
            100.0 * sum(case when o.is_on_time_delivery then 1 else 0 end)
            / nullif(count(case when o.days_to_deliver is not null then 1 end), 0),
            1
        )                                               as on_time_delivery_pct,
        count(case when o.review_sentiment = 'negative' then 1 end)
                                                        as negative_review_count
    from order_items oi
    inner join orders o on oi.order_id = o.order_id
    group by oi.seller_id
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['s.seller_id']) }} as seller_sk,
        s.seller_id,
        s.city                                          as seller_city,
        s.state                                         as seller_state,
        coalesce(m.total_orders,            0)          as total_orders,
        coalesce(m.total_items_sold,        0)          as total_items_sold,
        coalesce(m.unique_products,         0)          as unique_products,
        coalesce(m.total_revenue,           0)          as total_revenue,
        coalesce(m.avg_item_price,          0)          as avg_item_price,
        m.avg_review_score,
        m.avg_days_to_deliver,
        coalesce(m.on_time_delivery_pct,    0)          as on_time_delivery_pct,
        coalesce(m.negative_review_count,   0)          as negative_review_count,

        -- Performance tier
        case
            when coalesce(m.total_revenue, 0) = 0    then 'inactive'
            when m.avg_review_score >= 4
             and m.on_time_delivery_pct >= 80         then 'top'
            when m.avg_review_score >= 3
             and m.on_time_delivery_pct >= 60         then 'standard'
            else                                           'at-risk'
        end as seller_tier,

        current_timestamp()                             as dbt_updated_at

    from sellers s
    left join seller_metrics m on s.seller_id = m.seller_id
)

select * from final
