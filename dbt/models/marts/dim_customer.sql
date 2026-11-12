-- One row per VERSION of a customer (SCD Type 2): a new row every time a tracked attribute
-- (membership tier, city, hashed email) changes, so facts can be joined to the attributes
-- the customer had when they placed the order, not the ones they have today.
--
-- Rebuilt from the full order history on every run instead of using `dbt snapshot`:
-- a snapshot only sees changes between two runs (it dates a change by when the job ran,
-- and cannot be rebuilt if lost), while order_events already holds every observation
-- with its exact timestamp.
--
-- Validity intervals are half-open: [effective_start_date, effective_end_date).
-- The current version has effective_end_date = null and is_current = true.

with observations as (
    -- One observation per order: all events of an order carry the same customer snapshot,
    -- taken when the order was placed.
    select distinct
        customer_id,
        created_at as observed_at,
        hashed_email,
        city,
        membership_tier
    from {{ ref('stg_order_events') }}
),

with_previous as (
    select
        *,
        lag(membership_tier) over w as prev_tier,
        lag(city)            over w as prev_city,
        lag(hashed_email)    over w as prev_email,
        row_number()         over w as observation_number
    from observations
    window w as (partition by customer_id order by observed_at)
),

changes as (
    -- Keep the first observation and every observation where something changed.
    -- "is distinct from" treats null = null as equal, unlike "<>".
    select *
    from with_previous
    where observation_number = 1
       or membership_tier is distinct from prev_tier
       or city            is distinct from prev_city
       or hashed_email    is distinct from prev_email
)

select
    {{ surrogate_key(['customer_id', 'observed_at']) }}                      as customer_key,
    customer_id,
    hashed_email,
    city,
    membership_tier,
    observed_at                                                              as effective_start_date,
    lead(observed_at) over (partition by customer_id order by observed_at)  as effective_end_date,
    lead(observed_at) over (partition by customer_id order by observed_at) is null as is_current
from changes
