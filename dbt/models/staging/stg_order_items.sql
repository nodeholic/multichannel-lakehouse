-- One row per product within an order: the grain of the Gold fact table.
select
    order_id,
    product_id,
    product_name,
    category,
    quantity,
    unit_price,
    gross_amount,
    discount_amount,
    net_amount,
    order_date,
    channel,
    customer_id,
    order_status
from {{ source('silver', 'order_items') }}
