"""Load Phase 1 reference entities into PostgreSQL raw tables."""

from __future__ import annotations

import csv
from datetime import date
from decimal import Decimal
from pathlib import Path
import sys

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPOSITORY_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

import psycopg

from finbank.postgres_config import PostgresConfig


def _read_csv(path: Path, required_fields: set[str]) -> list[dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fieldnames = set(reader.fieldnames or [])
        missing = required_fields - fieldnames
        if missing:
            raise ValueError(
                f"{path} is missing required columns: {', '.join(sorted(missing))}"
            )
        return list(reader)


def main() -> int:
    output_dir = REPOSITORY_ROOT / "data" / "generated"
    customer_path = output_dir / "customers.csv"
    account_path = output_dir / "accounts.csv"
    merchant_path = output_dir / "merchants.csv"

    try:
        config = PostgresConfig.from_environment()

        customers = _read_csv(
            customer_path,
            {
                "customer_id", "first_name", "last_name", "date_of_birth",
                "country", "city", "registration_date", "customer_segment",
            },
        )
        accounts = _read_csv(
            account_path,
            {
                "account_id", "customer_id", "account_type", "currency",
                "account_open_date", "initial_balance",
            },
        )
        merchants = _read_csv(
            merchant_path,
            {"merchant_id", "merchant_name", "merchant_category", "country", "city"},
        )

        with psycopg.connect(
            config.dsn,
            connect_timeout=config.connect_timeout,
            application_name="finbank-reference-loader",
        ) as connection:
            with connection.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO raw.customers (
                        customer_id, first_name, last_name, date_of_birth,
                        country, city, registration_date, customer_segment
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (customer_id) DO UPDATE SET
                        first_name = EXCLUDED.first_name,
                        last_name = EXCLUDED.last_name,
                        date_of_birth = EXCLUDED.date_of_birth,
                        country = EXCLUDED.country,
                        city = EXCLUDED.city,
                        registration_date = EXCLUDED.registration_date,
                        customer_segment = EXCLUDED.customer_segment
                    """,
                    [
                        (
                            row["customer_id"], row["first_name"], row["last_name"],
                            date.fromisoformat(row["date_of_birth"]), row["country"],
                            row["city"], date.fromisoformat(row["registration_date"]),
                            row["customer_segment"],
                        )
                        for row in customers
                    ],
                )

                cursor.executemany(
                    """
                    INSERT INTO raw.accounts (
                        account_id, customer_id, account_type, currency,
                        account_open_date, initial_balance
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (account_id) DO UPDATE SET
                        customer_id = EXCLUDED.customer_id,
                        account_type = EXCLUDED.account_type,
                        currency = EXCLUDED.currency,
                        account_open_date = EXCLUDED.account_open_date,
                        initial_balance = EXCLUDED.initial_balance
                    """,
                    [
                        (
                            row["account_id"], row["customer_id"], row["account_type"],
                            row["currency"], date.fromisoformat(row["account_open_date"]),
                            Decimal(row["initial_balance"]),
                        )
                        for row in accounts
                    ],
                )

                cursor.executemany(
                    """
                    INSERT INTO raw.merchants (
                        merchant_id, merchant_name, merchant_category, country, city
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (merchant_id) DO UPDATE SET
                        merchant_name = EXCLUDED.merchant_name,
                        merchant_category = EXCLUDED.merchant_category,
                        country = EXCLUDED.country,
                        city = EXCLUDED.city
                    """,
                    [
                        (
                            row["merchant_id"], row["merchant_name"],
                            row["merchant_category"], row["country"], row["city"],
                        )
                        for row in merchants
                    ],
                )

        print(
            "Loaded reference data: "
            f"customers={len(customers)} "
            f"accounts={len(accounts)} "
            f"merchants={len(merchants)}"
        )
        return 0

    except (OSError, ValueError, psycopg.Error) as exc:
        print(f"Reference-data load error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
