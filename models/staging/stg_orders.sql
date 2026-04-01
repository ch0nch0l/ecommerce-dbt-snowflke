-- STAGING: orders
-- ─────────────────────────────────────────────────────────────────────────────
-- Purpose : Clean and standardise the raw orders table.
--           Staging models are 1:1 with source tables — one model per source.
--           Rules: rename columns, cast types, basic filters only.
--           No joins, no business logic — that lives in intermediate/.
-- ─────────────────────────────────────────────────────────────────────────────

with source as (
    select * from {{ source('olist', 'orders') }}
),

renamed as (
    select
        -- keys
        order_id,
        customer_id,

        -- status
        order_status,

        -- timestamps — cast to proper timestamp type and rename for clarity
        cast(order_purchase_timestamp  as timestamp) as ordered_at,
        cast(order_approved_at         as timestamp) as approved_at,
        cast(order_delivered_carrier_date as timestamp) as shipped_at,
        cast(order_delivered_customer_date as timestamp) as delivered_at,
        cast(order_estimated_delivery_date as timestamp) as estimated_delivery_at,

        -- derived: days from purchase to delivery
        datediff(
            'day',
            cast(order_purchase_timestamp as timestamp),
            cast(order_delivered_customer_date as timestamp)
        ) as days_to_deliver,

        -- derived: was the order delivered on time?
        case
            when order_delivered_customer_date is null then null
            when order_delivered_customer_date <= order_estimated_delivery_date then true
            else false
        end as is_on_time_delivery

    from source
    where order_id is not null
)

select * from renamed
