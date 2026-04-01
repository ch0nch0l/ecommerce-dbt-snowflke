-- SINGULAR TEST: assert_review_score_range
-- All review scores must be between 1 and 5. Returns violating rows.

select
    review_id,
    order_id,
    review_score
from {{ ref('stg_order_reviews') }}
where review_score not between 1 and 5
