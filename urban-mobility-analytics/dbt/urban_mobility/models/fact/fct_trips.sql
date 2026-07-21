{{
    config(
        materialized='incremental',
        unique_key='pickup_year_month',
        incremental_strategy='delete+insert',
        indexes=[{'columns': ['pickup_year_month'], 'type': 'btree'}]
    )
}}

with trips as (
    select
        *,
        -- desempata corridas idênticas em todas as colunas do hash (ex: taxas
        -- de despacho de aeroporto, várias no mesmo instante) pra garantir
        -- trip_sk único mesmo quando os dados de origem não distinguem as linhas
        row_number() over (
            partition by
                vendor_id, tpep_pickup_datetime, tpep_dropoff_datetime,
                pu_location_id, do_location_id, payment_type,
                passenger_count, trip_distance, total_amount
            order by tpep_pickup_datetime
        ) as dedup_seq
    from {{ ref('stg_trips') }}
    {% if is_incremental() and var('year_month', none) %}
    where pickup_year_month = '{{ var("year_month") }}'
    {% endif %}
),

payment as (
    select payment_type from {{ ref('dim_payment_type') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['trips.vendor_id', 'trips.tpep_pickup_datetime', 'trips.tpep_dropoff_datetime', 'trips.pu_location_id', 'trips.do_location_id', 'trips.payment_type', 'trips.passenger_count', 'trips.trip_distance', 'trips.total_amount', 'trips.dedup_seq']) }} as trip_sk,
    dim_date.date_id,
    trips.pickup_year_month,
    trips.vendor_id,
    coalesce(payment.payment_type, -1) as payment_type_id, -- sem match na referência vira Unknown
    trips.pu_location_id,
    trips.do_location_id,
    trips.passenger_count,
    trips.trip_distance,
    trips.trip_duration_minutes,
    trips.fare_amount,
    trips.tip_amount,
    trips.tolls_amount,
    trips.total_amount,
    trips.valid_revenue,
    trips.is_valid_trip,
    trips.invalid_reason,
    trips.is_anomaly,
    trips.anomaly_reason
from trips
inner join {{ ref('dim_date') }} as dim_date
    on trips.pickup_date = dim_date.date_day
left join payment
    on trips.payment_type = payment.payment_type