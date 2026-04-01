-- STAGING: customers
-- ─────────────────────────────────────────────────────────────────────────────
-- Important nuance: customer_id is per-order (a customer gets a new ID
-- for each order). customer_unique_id is the stable cross-order identifier.
-- ─────────────────────────────────────────────────────────────────────────────

with source as (
    select * from {{ source('olist', 'customers') }}
),

renamed as (
    select
        customer_id,           -- per-order customer key (FK in orders table)
        customer_unique_id,    -- stable real-world customer identifier
        customer_zip_code_prefix as zip_code_prefix,
        customer_city           as city,
        customer_state          as state
    from source
    where customer_id is not null
)

select * from renamed
