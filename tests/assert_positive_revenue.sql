-- SINGULAR TEST: assert_positive_revenue
-- ─────────────────────────────────────────────────────────────────────────────
-- Purpose : Ensure no orders in fct_orders have negative gross revenue.
--           dbt singular tests fail if the query returns ANY rows.
--           So this query returns rows that VIOLATE the rule.
-- ─────────────────────────────────────────────────────────────────────────────

select
    order_id,
    order_gross_revenue,
    order_status
from {{ ref('fct_orders') }}
where order_gross_revenue < 0
  and order_status not in ('canceled', 'unavailable')
