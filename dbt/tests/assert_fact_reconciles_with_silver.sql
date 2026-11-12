-- The fact table must contain exactly the same sales as Silver: same row count and same
-- money totals. Catches join fan-out (one sale matching two dimension rows and being
-- counted twice) and rows lost by a join. Returns a row, and fails, on any difference.
with fact as (
    select count(*) as n, sum(gross_amount) as gross, sum(discount_amount) as discount, sum(net_amount) as net
    from {{ ref('fact_order_items') }}
),

silver as (
    select count(*) as n, sum(gross_amount) as gross, sum(discount_amount) as discount, sum(net_amount) as net
    from {{ ref('stg_order_items') }}
)

select fact.*, silver.n as silver_n, silver.net as silver_net
from fact, silver
where fact.n <> silver.n
   or fact.gross <> silver.gross
   or fact.discount <> silver.discount
   or fact.net <> silver.net
