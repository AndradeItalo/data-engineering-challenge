from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(PROJECT_ROOT / ".env", override=False)


def _env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Variável de ambiente obrigatória não definida: {name}")
    return value


@dataclass(frozen=True)
class PostgresConfig:
    host: str
    port: int
    database: str
    user: str
    password: str

    @classmethod
    def from_env(cls) -> "PostgresConfig":
        return cls(
            host=_env("POSTGRES_HOST", "localhost"),
            port=int(_env("POSTGRES_PORT", "5432")),
            database=_env("POSTGRES_DB", "urban_mobility"),
            user=_env("POSTGRES_USER", "urban_mobility"),
            password=_env("POSTGRES_PASSWORD", "urban_mobility"),
        )

    @property
    def sqlalchemy_uri(self) -> str:
        return (
            f"postgresql+psycopg2://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"


@dataclass(frozen=True)
class LakeConfig:
    bronze_path: Path
    silver_path: Path

    @classmethod
    def from_env(cls) -> "LakeConfig":
        return cls(
            bronze_path=Path(_env("LAKE_BRONZE_PATH", "./data/bronze")).resolve(),
            silver_path=Path(_env("LAKE_SILVER_PATH", "./data/silver")).resolve(),
        )


@dataclass(frozen=True)
class TLCConfig:
    base_url: str
    dataset: str

    @classmethod
    def from_env(cls) -> "TLCConfig":
        return cls(
            base_url=_env("TLC_BASE_URL", "https://d37ci6vzurychx.cloudfront.net/trip-data"),
            dataset=_env("TLC_DATASET", "yellow"),
        )

    def file_name(self, year: int, month: int) -> str:
        return f"{self.dataset}_tripdata_{year:04d}-{month:02d}.parquet"

    def url(self, year: int, month: int) -> str:
        return f"{self.base_url}/{self.file_name(year, month)}"


@dataclass(frozen=True)
class DefaultCompetency:
    """Competência (ano/mês) usada quando a ingestão é chamada sem argumentos."""

    year: int
    month: int

    @classmethod
    def from_env(cls) -> "DefaultCompetency":
        return cls(
            year=int(_env("INGESTION_YEAR", "2025")),
            month=int(_env("INGESTION_MONTH", "01")),
        )
