-- The center of the star schema. Grain: one product within an order.
-- Holds only numbers (quantity, money) and keys pointing to the dimensions,
-- plus order_id and order_status as degenerate dimensions (attributes with no table of their own).
--
-- Customer attributes are joined "as of" the moment the order was placed: the version of
-- dim_customer whose half-open interval [effective_start_date, effective_end_date) contains
-- created_at. Exactly one version can match, so the join never multiplies rows.

with items as (
    select
        i.order_id,
        i.product_id,
        i.channel,
        i.customer_id,
        i.order_status,
        i.order_date,
        o.created_at,
        i.quantity,
        i.unit_price,
        i.gross_amount,
        i.discount_amount,
        i.net_amount
    from {{ ref('stg_order_items') }} as i
    join {{ ref('stg_orders') }} as o using (order_id)
)

select
    {{ surrogate_key(['items.order_id', 'items.product_id']) }} as order_item_key,
    items.order_id,
    cast(strftime(items.order_date, '%Y%m%d') as integer)    as date_key,
    c.customer_key,
    ch.channel_key,
    p.product_key,
    items.order_status,
    items.quantity,
    items.unit_price,
    items.gross_amount,
    items.discount_amount,
    items.net_amount
from items
left join {{ ref('dim_customer') }} as c
    on  c.customer_id = items.customer_id
    and items.created_at >= c.effective_start_date
    and (items.created_at < c.effective_end_date or c.effective_end_date is null)
left join {{ ref('dim_channel') }} as ch
    on ch.channel_code = items.channel
left join {{ ref('dim_product') }} as p
    on p.product_id = items.product_id
