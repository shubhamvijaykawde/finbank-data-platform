"""Validate runtime prerequisites for the FinBank workflow."""

from __future__ import annotations

from pathlib import Path
import shutil
import sys

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPOSITORY_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

import psycopg

from finbank.postgres_config import PostgresConfig


def main() -> int:
    required_files = (
        REPOSITORY_ROOT / "data" / "generated" / "customers.csv",
        REPOSITORY_ROOT / "data" / "generated" / "accounts.csv",
        REPOSITORY_ROOT / "data" / "generated" / "merchants.csv",
        REPOSITORY_ROOT / "data" / "generated" / "transactions.csv",
        REPOSITORY_ROOT / "dbt" / "dbt_project.yml",
    )

    missing = [str(path) for path in required_files if not path.exists()]
    if missing:
        print("Pipeline validation failed: missing prerequisite files:")
        for path in missing:
            print(f"  {path}")
        return 1

    if shutil.which("dbt") is None:
        print("Pipeline validation failed: dbt executable not found on PATH")
        return 1

    try:
        config = PostgresConfig.from_environment()
        with psycopg.connect(
            config.dsn,
            connect_timeout=config.connect_timeout,
            application_name="finbank-pipeline-validation",
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT current_database(), current_user")
                database, user = cursor.fetchone()

                cursor.execute(
                    """
                    SELECT table_schema, table_name
                    FROM information_schema.tables
                    WHERE (table_schema, table_name) IN (
                        ('raw', 'customers'),
                        ('raw', 'accounts'),
                        ('raw', 'merchants'),
                        ('raw', 'transaction_events'),
                        ('raw', 'rejected_records')
                    )
                    ORDER BY table_schema, table_name
                    """
                )
                relations = cursor.fetchall()

        expected_relations = {
            ("raw", "customers"),
            ("raw", "accounts"),
            ("raw", "merchants"),
            ("raw", "transaction_events"),
            ("raw", "rejected_records"),
        }
        actual_relations = set(relations)
        missing_relations = expected_relations - actual_relations

        if missing_relations:
            print(
                "Pipeline validation failed: missing PostgreSQL relations: "
                + ", ".join(f"{schema}.{table}" for schema, table in sorted(missing_relations))
            )
            return 1

        print(
            "Pipeline validation passed: "
            f"database={database} user={user} "
            f"raw_relations={len(actual_relations)}"
        )
        return 0

    except (OSError, ValueError, psycopg.Error) as exc:
        print(f"Pipeline validation failed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
