-- MART: fct_orders
-- ─────────────────────────────────────────────────────────────────────────────
-- Purpose : Core order fact table. The primary table for all revenue and
--           order-level reporting. Consumed directly by BI tools.
--
-- Grain        : One row per order
-- Materialized : incremental — only processes new orders on each run,
--                making this efficient for large and growing datasets.
--
-- Interview tip: Incremental models are one of the most important dbt
-- concepts. This pattern shows you understand production-scale dbt.
-- ─────────────────────────────────────────────────────────────────────────────

{{
  config(
    materialized = 'incremental',
    unique_key   = 'order_id',
    on_schema_change = 'sync_all_columns'
  )
}}

with orders as (
    select * from {{ ref('int_orders_enriched') }}

    -- Incremental filter: on each run, only process orders newer than
    -- the latest order already in the table.
    {% if is_incremental() %}
      where ordered_at > (select max(ordered_at) from {{ this }})
    {% endif %}
),

final as (
    select
        -- Keys
        order_id,
        customer_id,
        customer_unique_id,

        -- Dimensions
        order_status,
        customer_state,
        customer_city,
        primary_payment_type,
        review_sentiment,

        -- Date dimensions
        order_date,
        order_month,
        order_year,
        order_dow,
        order_hour,

        -- Timestamps
        ordered_at,
        approved_at,
        shipped_at,
        delivered_at,
        estimated_delivery_at,

        -- Measures
        item_count,
        distinct_products,
        distinct_sellers,
        items_revenue,
        total_freight,
        order_gross_revenue,
        total_payment,
        avg_item_price,

        -- Delivery metrics
        days_to_deliver,
        is_on_time_delivery,

        -- Payment detail
        payment_method_count,
        max_installments,
        is_split_payment,

        -- Review metrics
        review_score,
        review_created_at,

        -- Audit
        current_timestamp() as dbt_updated_at

    from orders
    -- Only include orders that have been placed (exclude ghost rows)
    where order_id is not null
)

select * from final
