-- One row per order, latest known state.
-- Staging selects columns explicitly: if Silver adds or renames a column,
-- only this file changes and every downstream model keeps working.
select
    order_id,
    channel,
    order_status,
    created_at,
    updated_at,
    order_date,
    customer_id,
    item_count,
    gross_amount,
    discount_amount,
    net_amount
from {{ source('silver', 'orders') }}
