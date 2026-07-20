from pathlib import Path
import requests

import pandas as pd
from sqlalchemy import text
import io

import argparse
from src.common.db import get_engine
from src.common.settings import DefaultCompetency, LakeConfig, PostgresConfig, TLCConfig


def download_parquet(year: int, month: int, bronze_path: Path, base_url: str, force: bool = False) -> Path:
    file_name = f"yellow_tripdata_{year:04d}-{month:02d}.parquet"
    target = bronze_path / file_name
    bronze_path.mkdir(parents=True, exist_ok=True)

    if target.exists() and not force:
        print(f"{target.name} já existe, pulando download.")
        return target

    url = f"{base_url}/{file_name}"
    part = target.with_suffix(target.suffix + ".part") # garantindo que se a conexão cair no download vai estar no .part, protege um futuro target.exist

    print(f"Baixando {url}")
    with requests.get(url, stream=True, timeout=60) as response:
        response.raise_for_status()
        with open(part, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)

    part.rename(target)
    print(f"Salvo em {target}")
    return target


COLUMN_MAP = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "tpep_pickup_datetime",
    "tpep_dropoff_datetime": "tpep_dropoff_datetime",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "RatecodeID": "rate_code_id",
    "store_and_fwd_flag": "store_and_fwd_flag",
    "PULocationID": "pu_location_id",
    "DOLocationID": "do_location_id",
    "payment_type": "payment_type",
    "fare_amount": "fare_amount",
    "extra": "extra",
    "mta_tax": "mta_tax",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "total_amount": "total_amount",
    "congestion_surcharge": "congestion_surcharge",
    "Airport_fee": "airport_fee",
    "cbd_congestion_fee": "cbd_congestion_fee",
}


def load_to_bronze(path: Path, year: int, month: int, engine) -> int:
    year_month = f"{year:04d}-{month:02d}"

    df = pd.read_parquet(path, engine="pyarrow", dtype_backend="numpy_nullable")
    df = df.rename(columns={k: v for k, v in COLUMN_MAP.items() if k in df.columns})
    df = df[[c for c in COLUMN_MAP.values() if c in df.columns]]
    df["source_file"] = path.name
    df["source_year_month"] = year_month

    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    columns = ", ".join(df.columns)
    # copy pra evitar overhead de parsing/validação por linha do insert
    copy_sql = f"COPY bronze.yellow_tripdata ({columns}) FROM STDIN WITH (FORMAT csv, NULL '')"

    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM bronze.yellow_tripdata WHERE source_year_month = :ym"),
            {"ym": year_month},
        )
        cursor = conn.connection.cursor()
        cursor.copy_expert(copy_sql, buffer)

    return len(df)


def main() -> None:
    default = DefaultCompetency.from_env()

    parser = argparse.ArgumentParser(description="Baixa e carrega dados da TLC na camada bronze.")
    parser.add_argument("--year", type=int, default=default.year)
    parser.add_argument("--month", type=int, choices=range(1, 13))
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    tlc = TLCConfig.from_env()
    lake = LakeConfig.from_env()
    engine = get_engine(PostgresConfig.from_env())

    months = [args.month] if args.month else list(range(1, 10))

    total = 0
    for month in months:
        path = download_parquet(args.year, month, lake.bronze_path, tlc.base_url, force=args.force)
        rows = load_to_bronze(path, args.year, month, engine)
        print(f"{args.year:04d}-{month:02d}: {rows} linhas")
        total += rows

    print(f"Total: {total} linhas em {len(months)} competência(s).")


if __name__ == "__main__":
    main()