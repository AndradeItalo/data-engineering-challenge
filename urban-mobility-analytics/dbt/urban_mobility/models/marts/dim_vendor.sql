with distinct_vendors as (
    select distinct vendor_id
    from {{ ref('stg_trips') }}
    where vendor_id is not null
)

select
    vendor_id,
    case vendor_id
        when 1 then 'Creative Mobile Technologies, LLC'
        when 2 then 'Curb Mobility, LLC'
        when 6 then 'Myle Technologies Inc'
        when 7 then 'Helix'
        else 'Unknown vendor'
    end as vendor_name
from distinct_vendors