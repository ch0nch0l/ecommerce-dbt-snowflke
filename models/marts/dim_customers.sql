-- MART: dim_customers
-- ─────────────────────────────────────────────────────────────────────────────
-- Purpose : Customer dimension table with lifetime value metrics.
--           Uses customer_unique_id as the stable grain — a real-world
--           customer can have many orders (each with a different customer_id).
--
-- Grain        : One row per unique customer (customer_unique_id)
-- Materialized : table
-- ─────────────────────────────────────────────────────────────────────────────

with orders as (
    select * from {{ ref('int_orders_enriched') }}
    where order_status not in ('canceled', 'unavailable')
),

customer_orders as (
    select
        customer_unique_id,
        customer_state,
        customer_city,

        -- Lifetime order metrics
        count(distinct order_id)                            as total_orders,
        sum(order_gross_revenue)                            as lifetime_revenue,
        avg(order_gross_revenue)                            as avg_order_value,
        sum(item_count)                                     as total_items_bought,

        -- Time metrics
        min(ordered_at)                                     as first_order_at,
        max(ordered_at)                                     as last_order_at,
        datediff('day', min(ordered_at), max(ordered_at))   as customer_lifespan_days,

        -- Review behaviour
        avg(review_score)                                   as avg_review_score,
        count(case when review_score is not null then 1 end) as review_count,

        -- Delivery experience
        avg(days_to_deliver)                                as avg_days_to_deliver,
        sum(case when is_on_time_delivery then 1 else 0 end) as on_time_deliveries,

        -- Payment behaviour
        max(primary_payment_type)                           as most_used_payment_type

    from orders
    group by
        customer_unique_id,
        customer_state,
        customer_city
),

final as (
    select
        -- Surrogate key — best practice for dimension tables
        {{ dbt_utils.generate_surrogate_key(['customer_unique_id']) }}
            as customer_sk,

        customer_unique_id,
        customer_state,
        customer_city,

        -- Order history
        total_orders,
        round(lifetime_revenue,    2)   as lifetime_revenue,
        round(avg_order_value,     2)   as avg_order_value,
        total_items_bought,

        -- Timestamps
        first_order_at,
        last_order_at,
        customer_lifespan_days,

        -- Customer segment based on order count
        case
            when total_orders = 1 then 'one-time'
            when total_orders <= 3 then 'occasional'
            when total_orders <= 7 then 'regular'
            else                        'loyal'
        end as customer_segment,

        -- LTV tier based on lifetime revenue
        case
            when lifetime_revenue <  100  then 'bronze'
            when lifetime_revenue <  500  then 'silver'
            when lifetime_revenue < 1000  then 'gold'
            else                               'platinum'
        end as ltv_tier,

        -- Satisfaction
        round(avg_review_score, 2)      as avg_review_score,
        review_count,

        -- Logistics
        round(avg_days_to_deliver, 1)   as avg_days_to_deliver,
        on_time_deliveries,

        -- Payment
        most_used_payment_type,

        -- Audit
        current_timestamp()             as dbt_updated_at

    from customer_orders
)

select * from final
