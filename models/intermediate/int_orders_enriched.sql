-- INTERMEDIATE: orders_enriched
-- ─────────────────────────────────────────────────────────────────────────────
-- Purpose : Join orders with customers, aggregate items and payments,
--           and build the enriched order-level dataset used by all marts.
--           This is the most important model in the project.
--
-- Grain   : One row per order
-- ─────────────────────────────────────────────────────────────────────────────

with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

-- Aggregate items to order level
order_item_agg as (
    select
        order_id,
        count(*)                        as item_count,
        sum(price)                      as items_revenue,
        sum(freight_value)              as total_freight,
        sum(line_total)                 as order_gross_revenue,
        avg(price)                      as avg_item_price,
        count(distinct seller_id)       as distinct_sellers,
        count(distinct product_id)      as distinct_products
    from {{ ref('stg_order_items') }}
    group by order_id
),

-- Aggregate payments to order level
order_payment_agg as (
    select
        order_id,
        sum(payment_value)                              as total_payment,
        count(distinct payment_type)                    as payment_method_count,
        -- Primary payment method = highest value payment
        max_by(payment_type, payment_value)             as primary_payment_type,
        max(payment_installments)                       as max_installments,
        -- Flag if customer used multiple payment methods
        count(distinct payment_type) > 1                as is_split_payment
    from {{ ref('stg_order_payments') }}
    group by order_id
),

-- Latest review per order (some orders have multiple reviews)
order_review as (
    select
        order_id,
        max(review_score)                               as review_score,
        max(review_sentiment)                           as review_sentiment,
        max(review_created_at)                          as review_created_at
    from {{ ref('stg_order_reviews') }}
    group by order_id
),

joined as (
    select
        -- Order keys
        o.order_id,
        o.customer_id,
        c.customer_unique_id,
        c.state                                         as customer_state,
        c.city                                          as customer_city,

        -- Order metadata
        o.order_status,
        o.ordered_at,
        o.approved_at,
        o.shipped_at,
        o.delivered_at,
        o.estimated_delivery_at,
        o.days_to_deliver,
        o.is_on_time_delivery,

        -- Date parts for aggregation
        date_trunc('day',   o.ordered_at)               as order_date,
        date_trunc('month', o.ordered_at)               as order_month,
        date_trunc('year',  o.ordered_at)               as order_year,
        dayofweek(o.ordered_at)                         as order_dow,
        hour(o.ordered_at)                              as order_hour,

        -- Items
        coalesce(i.item_count,          0)              as item_count,
        coalesce(i.items_revenue,       0)              as items_revenue,
        coalesce(i.total_freight,       0)              as total_freight,
        coalesce(i.order_gross_revenue, 0)              as order_gross_revenue,
        coalesce(i.avg_item_price,      0)              as avg_item_price,
        coalesce(i.distinct_sellers,    0)              as distinct_sellers,
        coalesce(i.distinct_products,   0)              as distinct_products,

        -- Payments
        coalesce(p.total_payment,       0)              as total_payment,
        p.primary_payment_type,
        coalesce(p.payment_method_count, 0)             as payment_method_count,
        coalesce(p.max_installments,    1)              as max_installments,
        coalesce(p.is_split_payment,    false)          as is_split_payment,

        -- Reviews
        r.review_score,
        r.review_sentiment,
        r.review_created_at

    from orders o
    left join customers          c on o.customer_id   = c.customer_id
    left join order_item_agg     i on o.order_id      = i.order_id
    left join order_payment_agg  p on o.order_id      = p.order_id
    left join order_review       r on o.order_id      = r.order_id
)

select * from joined
