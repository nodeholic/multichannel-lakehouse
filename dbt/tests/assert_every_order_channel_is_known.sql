-- Singular test: returns the channels that appear in orders but are missing from the
-- channels seed. Any row returned fails the test, reminding us to add the new channel
-- to seeds/channels.csv before it reaches Gold.
select distinct o.channel
from {{ ref('stg_orders') }} as o
left join {{ ref('dim_channel') }} as c
    on o.channel = c.channel_code
where c.channel_code is null
