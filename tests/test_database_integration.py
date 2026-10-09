from __future__ import annotations

import os
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from finbank.kafka_events import build_transaction_event
from finbank.postgres import INSERT_TRANSACTION_EVENT_SQL
from finbank.postgres_config import PostgresConfig


pytestmark = pytest.mark.integration


def _integration_config() -> PostgresConfig:
    if os.getenv("FINBANK_RUN_DB_INTEGRATION_TESTS") != "1":
        pytest.skip("set FINBANK_RUN_DB_INTEGRATION_TESTS=1 to run PostgreSQL integration tests")
    return PostgresConfig.from_environment()


def test_real_postgres_insert_constraints_and_duplicate_handling() -> None:
    config = _integration_config()
    suffix = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
    transaction_id = f"PHASE11_DB_{suffix}"
    event_id = f"PHASE11_EVENT_{suffix}"

    import psycopg
    from psycopg.errors import CheckViolation
    from psycopg.types.json import Jsonb

    with psycopg.connect(config.dsn, connect_timeout=config.connect_timeout) as connection:
        with connection.cursor() as cursor:
            # Resolve valid foreign-key values from the live reference tables rather
            # than coupling this integration test to a generator's ID formatting.
            cursor.execute(
                "SELECT customer_id FROM raw.customers ORDER BY customer_id LIMIT 1"
            )
            customer_row = cursor.fetchone()
            assert customer_row is not None, (
                "raw.customers is empty; initialize the database reference data before "
                "running PostgreSQL integration tests"
            )
            customer_id = customer_row[0]

            cursor.execute(
                "SELECT account_id FROM raw.accounts ORDER BY account_id LIMIT 1"
            )
            account_row = cursor.fetchone()
            assert account_row is not None, (
                "raw.accounts is empty; initialize the database reference data before "
                "running PostgreSQL integration tests"
            )
            account_id = account_row[0]

            cursor.execute(
                "SELECT merchant_id FROM raw.merchants ORDER BY merchant_id LIMIT 1"
            )
            merchant_row = cursor.fetchone()
            assert merchant_row is not None, (
                "raw.merchants is empty; initialize the database reference data before "
                "running PostgreSQL integration tests"
            )
            merchant_id = merchant_row[0]

            event = build_transaction_event(
                {
                    "transaction_id": transaction_id,
                    "customer_id": customer_id,
                    "account_id": account_id,
                    "merchant_id": merchant_id,
                    "transaction_type": "purchase",
                    "amount": "25.00",
                    "currency": "EUR",
                    "timestamp": "2025-12-30T12:30:00+00:00",
                    "country": "Germany",
                    "city": "Berlin",
                    "payment_method": "card",
                    "status": "completed",
                },
                event_id=event_id,
            )
            params = {
                "event_id": event["event_id"],
                "transaction_id": transaction_id,
                "event_type": event["event_type"],
                "schema_version": event["schema_version"],
                "event_timestamp": datetime(2025, 12, 30, 12, 30, tzinfo=timezone.utc),
                "transaction_timestamp": datetime(2025, 12, 30, 12, 30, tzinfo=timezone.utc),
                "customer_id": customer_id,
                "account_id": account_id,
                "merchant_id": merchant_id,
                "amount": Decimal("25.00"),
                "currency": "EUR",
                "transaction_type": "purchase",
                "country": "Germany",
                "city": "Berlin",
                "payment_method": "card",
                "status": "completed",
                "payload": Jsonb(event),
                "kafka_topic": f"phase11-db-{suffix}",
                "kafka_partition": 0,
                "kafka_offset": 1,
            }

            cursor.execute(INSERT_TRANSACTION_EVENT_SQL, params)
            assert cursor.fetchone() == (event_id,)

            # Re-delivering the same event must not create a duplicate row.
            cursor.execute(INSERT_TRANSACTION_EVENT_SQL, params)
            assert cursor.fetchone() is None

            # The table's amount check constraint must reject a non-positive value.
            bad_params = dict(params)
            bad_params["event_id"] = f"PHASE11_BAD_{suffix}"
            bad_params["transaction_id"] = f"PHASE11_BAD_TXN_{suffix}"
            bad_params["amount"] = Decimal("0.00")
            with pytest.raises(CheckViolation):
                cursor.execute(INSERT_TRANSACTION_EVENT_SQL, bad_params)

        # Keep this integration test isolated: remove its inserted row on success.
        connection.rollback()
