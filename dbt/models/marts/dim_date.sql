-- One row per calendar day. A date dimension lists EVERY day, including days with no sales,
-- so reports can show "0 orders on Sunday" instead of silently skipping that day.
-- The range covers whole years around the order dates, so keys never shift as data grows.
-- date_key is an integer YYYYMMDD: readable, sortable, and the conventional Kimball format.
with bounds as (
    select
        make_date(year(min(order_date)), 1, 1)   as start_date,
        make_date(year(max(order_date)), 12, 31) as end_date
    from {{ ref('stg_orders') }}
),

days as (
    select cast(d as date) as full_date
    from bounds, generate_series(start_date, end_date, interval 1 day) as t(d)
)

select
    cast(strftime(full_date, '%Y%m%d') as integer) as date_key,
    full_date,
    isodow(full_date)                              as day_of_week,  -- 1 = Monday ... 7 = Sunday
    dayname(full_date)                             as day_name,
    day(full_date)                                 as day_of_month,
    month(full_date)                               as month,
    monthname(full_date)                           as month_name,
    quarter(full_date)                             as quarter,
    year(full_date)                                as year,
    isodow(full_date) in (6, 7)                    as is_weekend
from days
