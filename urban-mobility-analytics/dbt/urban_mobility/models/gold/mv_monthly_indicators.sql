{{ config(materialized='materialized_view') }}

select
    vendor_id,
    cast(replace(pickup_year_month, '-', '') as integer) as year_month,
    count(*) as total_trips,
    sum(valid_revenue) as total_valid_revenue,
    avg(total_amount) as avg_ticket, -- valor cobrado, não só a receita válida
    avg(trip_distance) as avg_distance
from {{ ref('fct_trips') }}
where is_valid_trip
group by vendor_id, pickup_year_month