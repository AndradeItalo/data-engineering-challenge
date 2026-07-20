from __future__ import annotations

from pathlib import Path
from typing import Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.common.settings import PostgresConfig

_engine: Optional[Engine] = None


def get_engine(config: Optional[PostgresConfig] = None) -> Engine:
    global _engine
    if _engine is None:
        cfg = config or PostgresConfig.from_env()
        _engine = create_engine(cfg.sqlalchemy_uri, pool_pre_ping=True, future=True)
    return _engine


def run_sql_file(path: Path, engine: Optional[Engine] = None) -> None:
    eng = engine or get_engine()
    sql = path.read_text(encoding="utf-8")
    with eng.begin() as conn:
        conn.execute(text(sql))


def table_count(schema: str, table: str, engine: Optional[Engine] = None) -> int:
    eng = engine or get_engine()
    with eng.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{table}"))
        return int(result.scalar_one())
