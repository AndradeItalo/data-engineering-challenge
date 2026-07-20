"""python -m src.common.bootstrap_db

Aplica os *.sql de sql/, src/bronze/sql/ e src/silver/sql/, nessa ordem
(schemas antes de tabelas). Idempotente (CREATE ... IF NOT EXISTS / ON CONFLICT).
"""
from __future__ import annotations

from pathlib import Path

from src.common.db import get_engine, run_sql_file
from src.common.settings import PostgresConfig

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
SQL_DIRS = [
    PROJECT_ROOT / "sql",
    PROJECT_ROOT / "src" / "bronze" / "sql",
    PROJECT_ROOT / "src" / "silver" / "sql",
]


def bootstrap() -> None:
    engine = get_engine(PostgresConfig.from_env())
    applied = 0
    for sql_dir in SQL_DIRS:
        for sql_file in sorted(sql_dir.glob("*.sql")):
            print(f"Aplicando {sql_file.relative_to(PROJECT_ROOT)} ...")
            run_sql_file(sql_file, engine)
            applied += 1
    print(f"Bootstrap concluído: {applied} script(s) aplicado(s).")


if __name__ == "__main__":
    bootstrap()
