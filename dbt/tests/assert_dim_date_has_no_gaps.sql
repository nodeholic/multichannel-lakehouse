-- Singular test: dim_date must contain every day between its first and last date.
-- Returns one row (and fails) if any day is missing.
select
    count(*)                                   as actual_days,
    max(full_date) - min(full_date) + 1        as expected_days
from {{ ref('dim_date') }}
having count(*) <> max(full_date) - min(full_date) + 1
