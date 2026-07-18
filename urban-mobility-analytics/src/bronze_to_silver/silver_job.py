from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from config.settings import PostgresConfig
import psycopg2
import argparse
from config.settings import DefaultCompetency, LakeConfig


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("urban_mobility_silver")
        .config("spark.jars", "/opt/spark-jars/postgresql-42.7.3.jar")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def read_bronze(spark: SparkSession, year: int, month: int, bronze_path: Path):
    file_name = f"yellow_tripdata_{year:04d}-{month:02d}.parquet"
    return spark.read.parquet(str(bronze_path / file_name))


def filter_competency(df, year: int, month: int):
    year_month = f"{year:04d}-{month:02d}"
    return df.filter(F.date_format("tpep_pickup_datetime", "yyyy-MM") == year_month)


def jdbc_properties(pg: PostgresConfig) -> dict:
    return {"user": pg.user, "password": pg.password, "driver": "org.postgresql.Driver"}


def read_payment_reference(spark: SparkSession, jdbc_url: str, properties: dict):
    return spark.read.jdbc(url=jdbc_url, table="bronze.payment_type_reference", properties=properties)


def join_payment_reference(df, payment_ref):
    return df.join(payment_ref, on="payment_type", how="left")


RENAME_MAP = {
    "VendorID": "vendor_id",
    "RatecodeID": "rate_code_id",
    "PULocationID": "pu_location_id",
    "DOLocationID": "do_location_id",
    "Airport_fee": "airport_fee",
}


def rename_columns(df):
    for old, new in RENAME_MAP.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)
    return df


def add_datetime_columns(df):
    df = df.withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
    df = df.withColumn("pickup_year_month", F.date_format("tpep_pickup_datetime", "yyyy-MM"))
    df = df.withColumn(
        "trip_duration_minutes",
        (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60.0,
    )
    return df


MAX_DURATION_MINUTES = 6 * 60
MAX_DISTANCE_MILES = 100.0


def add_quality_columns(df):
    rule_datetimes = F.col("tpep_pickup_datetime").isNotNull() & F.col("tpep_dropoff_datetime").isNotNull()
    rule_order = F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime")
    rule_distance = F.col("trip_distance") >= 0
    rule_amount = F.col("total_amount") >= 0
    rule_rate_code = F.col("rate_code_id").isNull() | F.col("rate_code_id").isin([1, 2, 3, 4, 5, 6])

    df = df.withColumn(
        "invalid_reason",
        F.concat_ws(
            ", ",
            F.when(~rule_datetimes, F.lit("datas de embarque/desembarque ausentes")),
            F.when(rule_datetimes & ~rule_order, F.lit("desembarque anterior ou igual ao embarque")),
            F.when(~rule_distance, F.lit("distância negativa")),
            F.when(~rule_amount, F.lit("valor total negativo")),
            F.when(~rule_rate_code, F.lit("tipo de tarifa fora do dicionário")),
        ),
    )
    df = df.withColumn(
        "invalid_reason",
        F.when(F.col("invalid_reason") == "", None).otherwise(F.col("invalid_reason")),
    )
    df = df.withColumn("is_valid_trip", F.col("invalid_reason").isNull())

    df = df.withColumn(
        "anomaly_reason",
        F.concat_ws(
            ", ",
            F.when(F.col("trip_duration_minutes") > MAX_DURATION_MINUTES, F.lit("duração acima de 6 horas")),
            F.when(F.col("trip_distance") > MAX_DISTANCE_MILES, F.lit("distância acima de 100 milhas")),
            F.when(F.col("passenger_count") == 0, F.lit("sem passageiro registrado")),
            F.when(
                (F.col("trip_distance") == 0) & (F.col("fare_amount") > 0),
                F.lit("distância zero com tarifa cobrada"),
            ),
        ),
    )
    df = df.withColumn(
        "anomaly_reason",
        F.when(F.col("anomaly_reason") == "", None).otherwise(F.col("anomaly_reason")),
    )
    df = df.withColumn("is_anomaly", F.col("anomaly_reason").isNotNull())

    df = df.withColumn(
        "valid_revenue",
        F.when(F.col("is_valid_payment") == True, F.col("total_amount")).otherwise(F.lit(0.0)), 
    )

    return df


FINAL_COLUMNS = [
    "vendor_id", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
    "trip_distance", "rate_code_id", "store_and_fwd_flag", "pu_location_id", "do_location_id",
    "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "total_amount", "congestion_surcharge", "airport_fee",
    "cbd_congestion_fee", "pickup_date", "pickup_year_month", "trip_duration_minutes",
    "is_valid_payment", "valid_revenue", "is_valid_trip", "invalid_reason",
    "is_anomaly", "anomaly_reason",
]


def select_output_columns(df):
    return df.select(*FINAL_COLUMNS)


def write_silver_parquet(df, silver_path: Path):
    (
        df.write
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("pickup_year_month")
        .parquet(str(silver_path))
    )


def write_silver_postgres(df, year: int, month: int, jdbc_url: str, properties: dict, pg: PostgresConfig):
    year_month = f"{year:04d}-{month:02d}"

    # DELETE numa conexão separada da escrita do Spark (não dá pra unir as duas
    # numa transação só). Reprocessar a competência de novo recupera qualquer falha.
    conn = psycopg2.connect(host=pg.host, port=pg.port, dbname=pg.database, user=pg.user, password=pg.password)
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM silver.trips WHERE pickup_year_month = %s", (year_month,))
        conn.commit()
    finally:
        conn.close()

    df.write.jdbc(url=jdbc_url, table="silver.trips", mode="append", properties=properties)


def process_month(spark, year, month, bronze_path, silver_path, jdbc_url, properties, pg):
    df = read_bronze(spark, year, month, bronze_path)
    df = filter_competency(df, year, month)
    payment_ref = read_payment_reference(spark, jdbc_url, properties)
    df = join_payment_reference(df, payment_ref)
    df = rename_columns(df)
    df = add_datetime_columns(df)
    df = add_quality_columns(df)
    df = select_output_columns(df)
    df.cache()

    write_silver_parquet(df, silver_path)
    write_silver_postgres(df, year, month, jdbc_url, properties, pg)

    total = df.count()
    valid = df.filter("is_valid_trip").count()
    anomalies = df.filter("is_anomaly").count()
    print(f"{year:04d}-{month:02d}: {total} linhas | {valid} válidas | {anomalies} anomalias")
    return total


def main():
    default = DefaultCompetency.from_env()

    parser = argparse.ArgumentParser(description="Processa a camada bronze -> silver.")
    parser.add_argument("--year", type=int, default=default.year)
    parser.add_argument("--month", type=int, choices=range(1, 13))
    args = parser.parse_args()

    pg = PostgresConfig.from_env()
    lake = LakeConfig.from_env()
    jdbc_url = pg.jdbc_url
    properties = jdbc_properties(pg)

    spark = build_spark_session()
    months = [args.month] if args.month else list(range(1, 13))

    total = 0
    for month in months:
        total += process_month(spark, args.year, month, lake.bronze_path, lake.silver_path, jdbc_url, properties, pg)

    print(f"Total: {total} linhas em {len(months)} competência(s).")


if __name__ == "__main__":
    main()