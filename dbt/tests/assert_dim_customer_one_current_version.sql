-- Every customer must have exactly one current version.
-- Returns the customers that have zero or several.
select customer_id, count(*) filter (where is_current) as current_versions
from {{ ref('dim_customer') }}
group by customer_id
having count(*) filter (where is_current) <> 1
