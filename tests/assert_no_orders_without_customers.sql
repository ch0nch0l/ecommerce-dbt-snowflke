-- SINGULAR TEST: assert_no_orders_without_customers
-- ─────────────────────────────────────────────────────────────────────────────
-- Every order in fct_orders must have a matching customer in dim_customers.
-- Returns rows that violate this rule — test fails if any rows returned.
-- ─────────────────────────────────────────────────────────────────────────────

select
    o.order_id,
    o.customer_unique_id
from {{ ref('fct_orders') }} o
left join {{ ref('dim_customers') }} c
    on o.customer_unique_id = c.customer_unique_id
where c.customer_unique_id is null
  and o.customer_unique_id is not null
