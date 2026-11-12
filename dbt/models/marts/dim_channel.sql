-- One row per sales channel, built from the channels seed (reference data kept in git).
select
    {{ surrogate_key(['channel_code']) }} as channel_key,
    channel_code,
    channel_name,
    platform_type
from {{ ref('channels') }}
