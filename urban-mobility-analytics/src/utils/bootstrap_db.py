"""python -m src.utils.bootstrap_db

Aplica sql/*.sql em ordem. Idempotente (CREATE ... IF NOT EXISTS / ON CONFLICT).
"""
from __future__ import annotations

import sys
from pathlib import Path

from config.settings import PostgresConfig
from src.utils.db import get_engine, run_sql_file

SQL_DIR = Path(__file__).resolve().parent.parent.parent / "sql"


def bootstrap() -> None:
    engine = get_engine(PostgresConfig.from_env())
    sql_files = sorted(SQL_DIR.glob("*.sql"))
    if not sql_files:
        print(f"Nenhum arquivo .sql encontrado em {SQL_DIR}", file=sys.stderr)
        return
    for sql_file in sql_files:
        print(f"Aplicando {sql_file.name} ...")
        run_sql_file(sql_file, engine)
    print(f"Bootstrap concluído: {len(sql_files)} script(s) aplicado(s).")


if __name__ == "__main__":
    bootstrap()
