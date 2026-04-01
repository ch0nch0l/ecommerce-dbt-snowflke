-- MART: mart_category_performance
-- ─────────────────────────────────────────────────────────────────────────────
-- Purpose : Revenue, volume, and satisfaction breakdown by product category.
--           Answers: which categories drive the most revenue? Which have the
--           best / worst customer satisfaction?
--
-- Grain   : One row per product category
-- ─────────────────────────────────────────────────────────────────────────────

with order_items as (
    select * from {{ ref('stg_order_items') }}
),

products as (
    select product_id, product_category from {{ ref('stg_products') }}
),

orders as (
    select
        order_id,
        order_status,
        review_score,
        review_sentiment,
        days_to_deliver,
        is_on_time_delivery,
        ordered_at
    from {{ ref('fct_orders') }}
    where order_status not in ('canceled', 'unavailable')
),

-- Join items to products to get category, then join to orders for metrics
item_enriched as (
    select
        oi.order_id,
        oi.product_id,
        oi.seller_id,
        oi.price,
        oi.freight_value,
        oi.line_total,
        p.product_category,
        o.review_score,
        o.review_sentiment,
        o.days_to_deliver,
        o.is_on_time_delivery,
        o.ordered_at
    from order_items oi
    left join products p on oi.product_id = p.product_id
    inner join orders  o on oi.order_id   = o.order_id
),

category_agg as (
    select
        coalesce(product_category, 'unknown') as product_category,

        -- Volume
        count(distinct order_id)              as total_orders,
        count(*)                              as total_items_sold,
        count(distinct product_id)            as unique_products,
        count(distinct seller_id)             as unique_sellers,

        -- Revenue
        round(sum(price),           2)        as total_revenue,
        round(sum(freight_value),   2)        as total_freight,
        round(sum(line_total),      2)        as total_gross_revenue,
        round(avg(price),           2)        as avg_item_price,
        round(min(price),           2)        as min_item_price,
        round(max(price),           2)        as max_item_price,

        -- Satisfaction
        round(avg(review_score),    2)        as avg_review_score,
        count(case when review_score is not null then 1 end) as review_count,
        count(case when review_sentiment = 'positive' then 1 end) as positive_reviews,
        count(case when review_sentiment = 'negative' then 1 end) as negative_reviews,

        -- Delivery
        round(avg(days_to_deliver), 1)        as avg_days_to_deliver,
        round(
            100.0 * sum(case when is_on_time_delivery then 1 else 0 end)
            / nullif(count(case when days_to_deliver is not null then 1 end), 0),
            1
        )                                     as on_time_delivery_pct

    from item_enriched
    group by product_category
),

final as (
    select
        *,
        -- Revenue share across all categories
        round(
            100.0 * total_revenue / sum(total_revenue) over (),
            2
        ) as revenue_share_pct,

        -- Sentiment rate
        round(
            100.0 * positive_reviews / nullif(review_count, 0),
            1
        ) as positive_review_pct,

        -- Revenue rank
        rank() over (order by total_revenue desc) as revenue_rank

    from category_agg
)

select * from final
order by revenue_rank
