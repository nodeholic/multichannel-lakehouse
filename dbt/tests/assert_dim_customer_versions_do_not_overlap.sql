-- Versions of one customer must not overlap: each version must end exactly where the
-- next one starts. Returns every pair of versions that breaks this.
with ordered as (
    select
        customer_id,
        effective_start_date,
        effective_end_date,
        lead(effective_start_date) over (
            partition by customer_id order by effective_start_date
        ) as next_start
    from {{ ref('dim_customer') }}
)

select *
from ordered
where effective_end_date is distinct from next_start
   or effective_end_date <= effective_start_date
