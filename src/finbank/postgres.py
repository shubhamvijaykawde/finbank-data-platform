"""PostgreSQL connection and idempotent raw-event persistence."""

from __future__ import annotations

import logging
import time
from datetime import datetime
from decimal import Decimal
from typing import Any

from finbank.postgres_config import PostgresConfig
from finbank.operational_events import record_operational_event

LOGGER = logging.getLogger(__name__)


INSERT_TRANSACTION_EVENT_SQL = """
INSERT INTO raw.transaction_events (
    event_id,
    transaction_id,
    event_type,
    schema_version,
    event_timestamp,
    transaction_timestamp,
    customer_id,
    account_id,
    merchant_id,
    amount,
    currency,
    transaction_type,
    country,
    city,
    payment_method,
    status,
    payload,
    kafka_topic,
    kafka_partition,
    kafka_offset
)
VALUES (
    %(event_id)s,
    %(transaction_id)s,
    %(event_type)s,
    %(schema_version)s,
    %(event_timestamp)s,
    %(transaction_timestamp)s,
    %(customer_id)s,
    %(account_id)s,
    %(merchant_id)s,
    %(amount)s,
    %(currency)s,
    %(transaction_type)s,
    %(country)s,
    %(city)s,
    %(payment_method)s,
    %(status)s,
    %(payload)s,
    %(kafka_topic)s,
    %(kafka_partition)s,
    %(kafka_offset)s
)
ON CONFLICT DO NOTHING
RETURNING event_id
"""


class RawTransactionWriter:
    """Own one PostgreSQL connection and retry transient DB failures."""

    def __init__(self, config: PostgresConfig) -> None:
        self.config = config
        self.connection: Any | None = None

    def connect(self) -> None:
        self.close()
        try:
            import psycopg
        except ImportError as exc:
            raise RuntimeError(
                "psycopg is not installed. Run: pip install -r requirements.txt"
            ) from exc

        try:
            self.connection = psycopg.connect(
                self.config.dsn,
                connect_timeout=self.config.connect_timeout,
                application_name="finbank-transaction-consumer",
            )
        except Exception as exc:
            record_operational_event(
                "error",
                "database",
                message=str(exc),
                error_class=type(exc).__name__,
                operation="database_connect",
            )
            raise

    def close(self) -> None:
        if self.connection is not None:
            try:
                self.connection.close()
            finally:
                self.connection = None

    def insert_event(
        self,
        event: dict[str, Any],
        kafka_topic: str,
        kafka_partition: int,
        kafka_offset: int,
    ) -> bool:
        """Persist one event atomically; return False when it is a duplicate."""
        transaction = event["transaction"]

        params = {
            "event_id": event["event_id"],
            "transaction_id": transaction["transaction_id"],
            "event_type": event["event_type"],
            "schema_version": event["schema_version"],
            "event_timestamp": _parse_timestamp(event["event_timestamp"]),
            "transaction_timestamp": _parse_timestamp(
                transaction["timestamp"]
            ),
            "customer_id": transaction["customer_id"],
            "account_id": transaction["account_id"],
            "merchant_id": transaction["merchant_id"],
            "amount": Decimal(transaction["amount"]),
            "currency": transaction["currency"],
            "transaction_type": transaction["transaction_type"],
            "country": transaction["country"],
            "city": transaction["city"],
            "payment_method": transaction["payment_method"],
            "status": transaction["status"],
            "payload": _jsonb(event),
            "kafka_topic": kafka_topic,
            "kafka_partition": kafka_partition,
            "kafka_offset": kafka_offset,
        }

        last_error: Exception | None = None

        for attempt in range(1, self.config.retry_attempts + 1):
            try:
                if self.connection is None or self.connection.closed:
                    self.connect()

                assert self.connection is not None

                with self.connection.transaction():
                    with self.connection.cursor() as cursor:
                        cursor.execute(
                            INSERT_TRANSACTION_EVENT_SQL,
                            params,
                        )
                        inserted = cursor.fetchone() is not None

                return inserted

            except Exception as exc:
                if not _is_integrity_error(exc):
                    record_operational_event(
                        "error",
                        "database",
                        message=str(exc),
                        error_class=type(exc).__name__,
                        operation="raw_transaction_insert",
                    )
                if not _is_transient_db_error(exc):
                    if _is_integrity_error(exc):
                        if self.connection is not None:
                            self.connection.rollback()
                        raise
                    raise

                last_error = exc
                self.close()

                if attempt >= self.config.retry_attempts:
                    break

                delay = self.config.retry_backoff_seconds * (2 ** (attempt - 1))
                LOGGER.warning(
                    "PostgreSQL transient failure on attempt %s/%s: %s; "
                    "retrying in %.1fs",
                    attempt,
                    self.config.retry_attempts,
                    exc,
                    delay,
                )
                time.sleep(delay)

        assert last_error is not None
        raise last_error


def _jsonb(value: dict[str, Any]) -> Any:
    try:
        from psycopg.types.json import Jsonb
    except ImportError as exc:
        raise RuntimeError(
            "psycopg is not installed. Run: pip install -r requirements.txt"
        ) from exc
    return Jsonb(value)


def _is_transient_db_error(exc: Exception) -> bool:
    try:
        from psycopg.errors import InterfaceError, OperationalError
    except ImportError:
        return False
    return isinstance(exc, (OperationalError, InterfaceError))


def _is_integrity_error(exc: Exception) -> bool:
    try:
        from psycopg.errors import IntegrityError
    except ImportError:
        return False
    return isinstance(exc, IntegrityError)


def _parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(
        value.replace("Z", "+00:00")
    )
