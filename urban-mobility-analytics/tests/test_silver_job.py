from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.silver.silver_job import (
    add_datetime_columns,
    add_quality_columns,
    rename_columns,
)

SCHEMA = StructType(
    [
        StructField("VendorID", IntegerType()),
        StructField("tpep_pickup_datetime", TimestampType()),
        StructField("tpep_dropoff_datetime", TimestampType()),
        StructField("passenger_count", LongType()),
        StructField("trip_distance", DoubleType()),
        StructField("RatecodeID", LongType()),
        StructField("store_and_fwd_flag", StringType()),
        StructField("PULocationID", IntegerType()),
        StructField("DOLocationID", IntegerType()),
        StructField("payment_type", LongType()),
        StructField("fare_amount", DoubleType()),
        StructField("extra", DoubleType()),
        StructField("mta_tax", DoubleType()),
        StructField("tip_amount", DoubleType()),
        StructField("tolls_amount", DoubleType()),
        StructField("improvement_surcharge", DoubleType()),
        StructField("total_amount", DoubleType()),
        StructField("congestion_surcharge", DoubleType()),
        StructField("Airport_fee", DoubleType()),
        StructField("cbd_congestion_fee", DoubleType()),
        StructField("is_valid_payment", BooleanType()),
    ]
)


@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.appName("test_silver_job").master("local[1]").getOrCreate()
    yield spark
    spark.stop()


def make_trip(spark, **overrides):
    base = dict(
        VendorID=1,
        tpep_pickup_datetime=datetime(2025, 2, 1, 10, 0, 0),
        tpep_dropoff_datetime=datetime(2025, 2, 1, 10, 20, 0),
        passenger_count=1,
        trip_distance=5.0,
        RatecodeID=1,
        store_and_fwd_flag="N",
        PULocationID=100,
        DOLocationID=200,
        payment_type=1,
        fare_amount=10.0,
        extra=0.0,
        mta_tax=0.5,
        tip_amount=2.0,
        tolls_amount=0.0,
        improvement_surcharge=0.3,
        total_amount=12.8,
        congestion_surcharge=2.5,
        Airport_fee=0.0,
        cbd_congestion_fee=0.75,
        is_valid_payment=True,
    )
    base.update(overrides)
    return spark.createDataFrame([base], schema=SCHEMA)


def process(df):
    df = rename_columns(df)
    df = add_datetime_columns(df)
    df = add_quality_columns(df)
    return df.collect()[0].asDict()


def test_valid_trip_passes_all_rules(spark):
    row = process(make_trip(spark))
    assert row["is_valid_trip"] is True
    assert row["invalid_reason"] is None
    assert row["is_anomaly"] is False
    assert row["anomaly_reason"] is None


def test_missing_pickup_datetime_invalid(spark):
    row = process(make_trip(spark, tpep_pickup_datetime=None))
    assert row["is_valid_trip"] is False
    assert "ausentes" in row["invalid_reason"]


def test_missing_dropoff_datetime_invalid(spark):
    row = process(make_trip(spark, tpep_dropoff_datetime=None))
    assert row["is_valid_trip"] is False
    assert "ausentes" in row["invalid_reason"]


def test_dropoff_before_pickup_invalid(spark):
    row = process(
        make_trip(
            spark,
            tpep_pickup_datetime=datetime(2025, 2, 1, 10, 20, 0),
            tpep_dropoff_datetime=datetime(2025, 2, 1, 10, 0, 0),
        )
    )
    assert row["is_valid_trip"] is False
    assert "desembarque anterior" in row["invalid_reason"]


def test_dropoff_equal_pickup_is_valid(spark):
    same_instant = datetime(2025, 2, 1, 10, 0, 0)
    row = process(make_trip(spark, tpep_pickup_datetime=same_instant, tpep_dropoff_datetime=same_instant))
    assert row["is_valid_trip"] is True
    assert row["trip_duration_minutes"] == 0.0


def test_negative_distance_invalid(spark):
    row = process(make_trip(spark, trip_distance=-1.0))
    assert row["is_valid_trip"] is False
    assert "distância negativa" in row["invalid_reason"]


def test_negative_total_amount_invalid(spark):
    row = process(make_trip(spark, total_amount=-5.0))
    assert row["is_valid_trip"] is False
    assert "valor total negativo" in row["invalid_reason"]


def test_unknown_rate_code_invalid(spark):
    row = process(make_trip(spark, RatecodeID=99))
    assert row["is_valid_trip"] is False
    assert "dicionário" in row["invalid_reason"]


def test_null_rate_code_is_valid(spark):
    row = process(make_trip(spark, RatecodeID=None))
    assert row["is_valid_trip"] is True


def test_long_duration_is_anomaly_not_invalid(spark):
    row = process(
        make_trip(
            spark,
            tpep_pickup_datetime=datetime(2025, 2, 1, 0, 0, 0),
            tpep_dropoff_datetime=datetime(2025, 2, 1, 7, 0, 0),
        )
    )
    assert row["is_valid_trip"] is True
    assert row["is_anomaly"] is True
    assert "duração acima de 6 horas" in row["anomaly_reason"]


def test_long_distance_is_anomaly_not_invalid(spark):
    row = process(make_trip(spark, trip_distance=150.0))
    assert row["is_valid_trip"] is True
    assert row["is_anomaly"] is True
    assert "distância acima de 100 milhas" in row["anomaly_reason"]


def test_zero_passenger_count_is_anomaly(spark):
    row = process(make_trip(spark, passenger_count=0))
    assert row["is_valid_trip"] is True
    assert row["is_anomaly"] is True
    assert "sem passageiro registrado" in row["anomaly_reason"]


def test_zero_distance_with_fare_is_anomaly(spark):
    row = process(make_trip(spark, trip_distance=0.0, fare_amount=10.0))
    assert row["is_valid_trip"] is True
    assert row["is_anomaly"] is True
    assert "distância zero com tarifa cobrada" in row["anomaly_reason"]


def test_valid_revenue_uses_total_amount_when_payment_valid(spark):
    row = process(make_trip(spark, is_valid_payment=True, total_amount=42.0))
    assert row["valid_revenue"] == 42.0


def test_valid_revenue_is_zero_when_payment_invalid(spark):
    row = process(make_trip(spark, is_valid_payment=False, total_amount=42.0))
    assert row["valid_revenue"] == 0.0


def test_rename_columns_maps_pascal_case_to_snake_case(spark):
    df = rename_columns(make_trip(spark))
    assert "vendor_id" in df.columns
    assert "rate_code_id" in df.columns
    assert "pu_location_id" in df.columns
    assert "do_location_id" in df.columns
    assert "airport_fee" in df.columns
    assert "VendorID" not in df.columns


def test_pickup_year_month_format(spark):
    row = process(make_trip(spark, tpep_pickup_datetime=datetime(2025, 7, 15, 8, 30, 0)))
    assert row["pickup_year_month"] == "2025-07"
    assert str(row["pickup_date"]) == "2025-07-15"
