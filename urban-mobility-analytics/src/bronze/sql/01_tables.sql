-- Espelha o schema do parquet "yellow_tripdata" da NYC TLC (2025), em snake_case,
-- mais colunas de linhagem para rastrear origem e permitir recarga incremental.
CREATE TABLE IF NOT EXISTS bronze.yellow_tripdata (
    vendor_id               INTEGER,
    tpep_pickup_datetime    TIMESTAMP,
    tpep_dropoff_datetime   TIMESTAMP,
    passenger_count         BIGINT,
    trip_distance           DOUBLE PRECISION,
    rate_code_id            BIGINT,
    store_and_fwd_flag      TEXT,
    pu_location_id          INTEGER,
    do_location_id          INTEGER,
    payment_type            BIGINT,
    fare_amount             DOUBLE PRECISION,
    extra                   DOUBLE PRECISION,
    mta_tax                 DOUBLE PRECISION,
    tip_amount              DOUBLE PRECISION,
    tolls_amount            DOUBLE PRECISION,
    improvement_surcharge   DOUBLE PRECISION,
    total_amount            DOUBLE PRECISION,
    congestion_surcharge    DOUBLE PRECISION,
    airport_fee             DOUBLE PRECISION,
    cbd_congestion_fee      DOUBLE PRECISION,
    source_file             TEXT NOT NULL,
    source_year_month       TEXT NOT NULL,
    ingested_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_yellow_tripdata_year_month
    ON bronze.yellow_tripdata (source_year_month);
