-- STAGING: order_items
-- ─────────────────────────────────────────────────────────────────────────────
-- One row per item within an order.
-- An order can contain multiple items from different sellers.
-- ─────────────────────────────────────────────────────────────────────────────

with source as (
    select * from {{ source('olist', 'order_items') }}
),

renamed as (
    select
        -- composite key: order_id + order_item_id uniquely identifies a line
        order_id,
        order_item_id,
        product_id,
        seller_id,

        -- amounts — keep original names, cast to numeric
        cast(price           as numeric(12, 2)) as price,
        cast(freight_value   as numeric(12, 2)) as freight_value,

        -- derived: total line amount
        cast(price as numeric(12, 2))
            + cast(freight_value as numeric(12, 2)) as line_total,

        -- timestamp
        cast(shipping_limit_date as timestamp) as shipping_limit_at

    from source
    where order_id is not null
      and order_item_id is not null
)

select * from renamed
