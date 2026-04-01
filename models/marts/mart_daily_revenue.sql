-- MART: mart_daily_revenue
-- ─────────────────────────────────────────────────────────────────────────────
-- Purpose : Daily revenue and order summary — the primary BI dashboard table.
--           Answers: how much did we make today? How many orders?
--
-- Grain   : One row per day
-- ─────────────────────────────────────────────────────────────────────────────

with orders as (
    select * from {{ ref('fct_orders') }}
    where order_status not in ('canceled', 'unavailable')
),

daily as (
    select
        order_date,
        date_trunc('month', order_date)         as order_month,
        dayofweek(order_date)                   as day_of_week,
        dayname(order_date)                     as day_name,

        -- Volume
        count(distinct order_id)                as total_orders,
        count(distinct customer_unique_id)      as unique_customers,
        sum(item_count)                         as total_items,

        -- Revenue
        round(sum(order_gross_revenue),  2)     as gross_revenue,
        round(sum(items_revenue),        2)     as items_revenue,
        round(sum(total_freight),        2)     as freight_revenue,
        round(avg(order_gross_revenue),  2)     as avg_order_value,
        round(avg(avg_item_price),       2)     as avg_item_price,

        -- Delivery
        round(avg(days_to_deliver),      1)     as avg_days_to_deliver,
        sum(case when is_on_time_delivery then 1 else 0 end) as on_time_count,
        round(
            100.0 * sum(case when is_on_time_delivery then 1 else 0 end)
            / nullif(count(case when days_to_deliver is not null then 1 end), 0),
            1
        )                                       as on_time_delivery_pct,

        -- Satisfaction
        round(avg(review_score),         2)     as avg_review_score,
        count(case when review_score is not null then 1 end) as reviews_received

    from orders
    group by order_date, date_trunc('month', order_date),
             dayofweek(order_date), dayname(order_date)
),

-- Add 7-day rolling average revenue (useful for smoothing noise)
with_rolling as (
    select
        *,
        round(
            avg(gross_revenue) over (
                order by order_date
                rows between 6 preceding and current row
            ),
            2
        ) as revenue_7day_rolling_avg,

        round(
            avg(total_orders) over (
                order by order_date
                rows between 6 preceding and current row
            ),
            1
        ) as orders_7day_rolling_avg

    from daily
)

select * from with_rolling
order by order_date
