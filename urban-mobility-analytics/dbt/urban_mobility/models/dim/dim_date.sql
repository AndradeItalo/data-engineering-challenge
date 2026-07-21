with distinct_dates as (
    select distinct pickup_date
    from {{ ref('stg_trips') }}
    where pickup_date is not null
)

select
    cast(to_char(pickup_date, 'YYYYMMDD') as integer) as date_id,
    pickup_date as date_day,
    extract(year from pickup_date)::int as year,
    extract(month from pickup_date)::int as month,
    extract(day from pickup_date)::int as day,
    to_char(pickup_date, 'YYYY-MM') as year_month,
    trim(to_char(pickup_date, 'Day')) as day_of_week,
    extract(isodow from pickup_date) in (6, 7) as is_weekend
from distinct_dates