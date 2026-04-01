-- SNAPSHOT: customer_snapshot
-- ─────────────────────────────────────────────────────────────────────────────
-- Purpose : Track historical changes to customer attributes using SCD Type 2.
--           When a customer changes their city or state, the old row is expired
--           and a new row is inserted — preserving full change history.
--
-- Strategy: check — dbt compares check_cols on each run. If any column
--           has changed, the old row gets dbt_valid_to set and a new row
--           is inserted with dbt_valid_from = now().
--
-- Interview tip: SCD Type 2 is asked in nearly every Finnish enterprise
-- data engineering interview (Konecranes, Nokia, Nordea, TietoEVRY).
-- Know this cold.
-- ─────────────────────────────────────────────────────────────────────────────

{% snapshot customer_snapshot %}

{{
    config(
        target_schema = 'snapshots',
        unique_key    = 'customer_unique_id',
        strategy      = 'check',
        check_cols    = ['city', 'state', 'zip_code_prefix']
    )
}}

-- Source: staging customers — we snapshot the canonical, cleaned version
select
    customer_unique_id,
    -- Take the most recent city/state for this customer
    -- (a customer_unique_id can have multiple customer_ids)
    max(city)             as city,
    max(state)            as state,
    max(zip_code_prefix)  as zip_code_prefix
from {{ ref('stg_customers') }}
group by customer_unique_id

{% endsnapshot %}

-- ─────────────────────────────────────────────────────────────────────────────
-- What dbt adds automatically:
--   dbt_scd_id        — surrogate key for the snapshot row
--   dbt_updated_at    — when this row was last checked
--   dbt_valid_from    — when this version became current
--   dbt_valid_to      — when this version was superseded (NULL = current)
--
-- To query current records only:
--   SELECT * FROM snapshots.customer_snapshot WHERE dbt_valid_to IS NULL
--
-- To query as-of a specific date:
--   SELECT * FROM snapshots.customer_snapshot
--   WHERE dbt_valid_from <= '2017-06-01'
--     AND (dbt_valid_to > '2017-06-01' OR dbt_valid_to IS NULL)
-- ─────────────────────────────────────────────────────────────────────────────
