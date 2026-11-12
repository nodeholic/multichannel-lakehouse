-- Each sale must point to the customer version that was valid when the order was placed:
-- the tier on that dim_customer row must equal the tier captured in the order's own events.
-- Returns every sale attributed to the wrong version.
with order_snapshot as (
    select distinct order_id, membership_tier
    from {{ ref('stg_order_events') }}
)

select f.order_id, s.membership_tier as tier_at_order_time, c.membership_tier as tier_in_fact
from {{ ref('fact_order_items') }} as f
join {{ ref('dim_customer') }} as c using (customer_key)
join order_snapshot as s using (order_id)
where c.membership_tier <> s.membership_tier
