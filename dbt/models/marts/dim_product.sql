-- One row per product (SCD Type 1: attributes always show the latest known value).
-- The source has no product catalog, so products are derived from what was sold;
-- arg_max picks the attributes seen on the most recent order date.
with products as (
    select
        product_id,
        arg_max(product_name, order_date) as product_name,
        arg_max(category, order_date)     as category,
        arg_max(unit_price, order_date)   as list_price,
        min(order_date)                   as first_sold_date
    from {{ ref('stg_order_items') }}
    group by product_id
)

select
    {{ surrogate_key(['product_id']) }} as product_key,
    product_id,
    product_name,
    category,
    list_price,
    first_sold_date
from products
