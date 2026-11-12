-- Every distinct status change of every order. Used to rebuild customer history (SCD2).
select
    event_id,
    order_id,
    order_status,
    updated_at,
    customer_id,
    hashed_email,
    city,
    membership_tier
from {{ source('silver', 'order_events') }}
