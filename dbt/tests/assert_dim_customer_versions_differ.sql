-- A new version must only exist when a tracked attribute changed.
-- Returns consecutive versions that are identical (a redundant version).
with ordered as (
    select
        *,
        lag(membership_tier) over w as prev_tier,
        lag(city)            over w as prev_city,
        lag(hashed_email)    over w as prev_email
    from {{ ref('dim_customer') }}
    window w as (partition by customer_id order by effective_start_date)
)

select customer_id, effective_start_date
from ordered
where prev_tier is not null
  and membership_tier is not distinct from prev_tier
  and city            is not distinct from prev_city
  and hashed_email    is not distinct from prev_email
