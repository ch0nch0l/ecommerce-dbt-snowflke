-- STAGING: order_payments
-- ─────────────────────────────────────────────────────────────────────────────
-- An order can be paid with multiple payment methods (e.g. voucher + card).
-- payment_sequential indicates the order of payments for a given order.
-- ─────────────────────────────────────────────────────────────────────────────

with source as (
    select * from {{ source('olist', 'order_payments') }}
),

renamed as (
    select
        order_id,
        cast(payment_sequential as int)         as payment_sequential,
        payment_type,
        cast(payment_installments as int)        as payment_installments,
        cast(payment_value as numeric(12, 2))    as payment_value
    from source
    where order_id is not null
)

select * from renamed
