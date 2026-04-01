-- MART: dim_products
-- ─────────────────────────────────────────────────────────────────────────────
-- Purpose : Product dimension with sales performance metrics.
-- Grain   : One row per product
-- ─────────────────────────────────────────────────────────────────────────────

with products as (
    select * from {{ ref('stg_products') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    -- Only count items from non-cancelled orders
    select order_id from {{ ref('stg_orders') }}
    where order_status not in ('canceled', 'unavailable')
),

product_sales as (
    select
        oi.product_id,
        count(*)                                as total_units_sold,
        count(distinct oi.order_id)             as total_orders,
        count(distinct oi.seller_id)            as sellers_carrying_product,
        sum(oi.price)                           as total_revenue,
        avg(oi.price)                           as avg_selling_price,
        min(oi.price)                           as min_selling_price,
        max(oi.price)                           as max_selling_price,
        avg(oi.freight_value)                   as avg_freight_value
    from order_items oi
    inner join orders o on oi.order_id = o.order_id
    group by oi.product_id
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['p.product_id']) }} as product_sk,

        p.product_id,
        p.product_category,
        p.product_category_portuguese,
        p.weight_grams,
        p.length_cm,
        p.height_cm,
        p.width_cm,
        p.product_photos_qty,

        -- Sales performance
        coalesce(s.total_units_sold,            0) as total_units_sold,
        coalesce(s.total_orders,                0) as total_orders,
        coalesce(s.sellers_carrying_product,    0) as sellers_carrying_product,
        coalesce(round(s.total_revenue,      2), 0) as total_revenue,
        coalesce(round(s.avg_selling_price,  2), 0) as avg_selling_price,
        coalesce(round(s.min_selling_price,  2), 0) as min_selling_price,
        coalesce(round(s.max_selling_price,  2), 0) as max_selling_price,
        coalesce(round(s.avg_freight_value,  2), 0) as avg_freight_value,

        -- Product tier by revenue
        case
            when coalesce(s.total_revenue, 0) = 0     then 'no sales'
            when s.total_revenue < 500                 then 'low'
            when s.total_revenue < 5000                then 'medium'
            else                                            'high'
        end as revenue_tier,

        current_timestamp() as dbt_updated_at

    from products p
    left join product_sales s on p.product_id = s.product_id
)

select * from final
