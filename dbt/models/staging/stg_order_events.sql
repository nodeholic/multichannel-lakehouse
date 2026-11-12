-- Every distinct status change of every order. Used to rebuild customer history (SCD2).
-- Customer attributes in an event are a snapshot taken when the order was PLACED (created_at),
-- not when its status changed (updated_at).
select
    event_id,
    order_id,
    order_status,
    created_at,
    updated_at,
    customer_id,
    hashed_email,
    city,
    membership_tier
from {{ source('silver', 'order_events') }}
