"""Initialize FinBank PostgreSQL schemas and raw tables."""

from __future__ import annotations

from pathlib import Path
import sys

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPOSITORY_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

import psycopg

from finbank.postgres_config import PostgresConfig


def main() -> int:
    try:
        config = PostgresConfig.from_environment()
        sql_paths = [
            REPOSITORY_ROOT / "sql" / "001_create_raw.sql",
            REPOSITORY_ROOT / "sql" / "003_create_rejected_records.sql",
            REPOSITORY_ROOT / "sql" / "008_create_monitoring.sql",
        ]

        statements: list[str] = []
        for sql_path in sql_paths:
            sql = sql_path.read_text(encoding="utf-8")
            statements.extend(
                statement.strip()
                for statement in sql.split(";")
                if statement.strip()
                and not all(
                    line.strip().startswith("--") or not line.strip()
                    for line in statement.splitlines()
                )
            )

        with psycopg.connect(
            config.dsn,
            connect_timeout=config.connect_timeout,
            application_name="finbank-db-init",
        ) as connection:
            with connection.cursor() as cursor:
                for statement in statements:
                    cursor.execute(statement)

        print("FinBank PostgreSQL schema initialized successfully.")
        print("Created/verified schemas: raw, staging, analytics, monitoring")
        return 0

    except (OSError, ValueError, psycopg.Error) as exc:
        print(f"Database initialization error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
