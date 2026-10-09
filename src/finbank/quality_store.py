"""PostgreSQL helpers for Phase 4 validation and quarantine."""

from __future__ import annotations

import logging
from typing import Any

from finbank.data_quality import (
    DUPLICATE_TRANSACTION,
    MISMATCHED_ACCOUNT_CUSTOMER,
    UNKNOWN_MERCHANT,
)
from finbank.postgres import RawTransactionWriter
from finbank.postgres_config import PostgresConfig
from finbank.operational_events import record_operational_event

LOGGER = logging.getLogger(__name__)

INSERT_REJECTED_RECORD_SQL = """
INSERT INTO raw.rejected_records (
    event_id,
    transaction_id,
    rejection_code,
    rejection_reason,
    raw_payload,
    kafka_topic,
    kafka_partition,
    kafka_offset
)
VALUES (
    %(event_id)s,
    %(transaction_id)s,
    %(rejection_code)s,
    %(rejection_reason)s,
    %(raw_payload)s,
    %(kafka_topic)s,
    %(kafka_partition)s,
    %(kafka_offset)s
)
ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING
RETURNING rejection_id
"""


class ReferenceDataValidator:
    """Check database-backed transaction relationships before ingestion."""

    def __init__(self, config: PostgresConfig) -> None:
        self.config = config
        self.writer = RawTransactionWriter(config)

    def close(self) -> None:
        self.writer.close()

    def validate(
        self,
        event: dict[str, Any],
    ) -> tuple[tuple[str, str], ...]:
        transaction = event["transaction"]
        customer_id = transaction.get("customer_id")
        account_id = transaction.get("account_id")
        merchant_id = transaction.get("merchant_id")
        transaction_id = transaction.get("transaction_id")

        if self.writer.connection is None or self.writer.connection.closed:
            self.writer.connect()

        assert self.writer.connection is not None

        try:
            with self.writer.connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        (c.customer_id IS NOT NULL) AS customer_exists,
                        a.customer_id AS account_customer_id,
                        (m.merchant_id IS NOT NULL) AS merchant_exists,
                        (te.transaction_id IS NOT NULL) AS transaction_exists
                    FROM (
                        VALUES (%s, %s, %s, %s)
                    ) AS input(customer_id, account_id, merchant_id, transaction_id)
                    LEFT JOIN raw.customers c
                        ON c.customer_id = input.customer_id
                    LEFT JOIN raw.accounts a
                        ON a.account_id = input.account_id
                    LEFT JOIN raw.merchants m
                        ON m.merchant_id = input.merchant_id
                    LEFT JOIN raw.transaction_events te
                        ON te.transaction_id = input.transaction_id
                    """,
                    (customer_id, account_id, merchant_id, transaction_id),
                )
                row = cursor.fetchone()
        except Exception as exc:
            record_operational_event(
                "error",
                "database",
                message=str(exc),
                error_class=type(exc).__name__,
                operation="reference_data_validation",
            )
            raise

        reasons: list[tuple[str, str]] = []

        customer_exists, account_customer_id, merchant_exists, transaction_exists = row

        if not merchant_exists:
            reasons.append((
                UNKNOWN_MERCHANT,
                f"merchant_id {merchant_id!r} does not exist in raw.merchants",
            ))

        if transaction_exists:
            reasons.append((
                DUPLICATE_TRANSACTION,
                f"transaction_id {transaction_id!r} already exists in raw.transaction_events",
            ))

        if account_customer_id is not None and account_customer_id != customer_id:
            reasons.append((
                MISMATCHED_ACCOUNT_CUSTOMER,
                f"account {account_id!r} belongs to customer {account_customer_id!r}, "
                f"not {customer_id!r}",
            ))

        if not customer_exists and customer_id:
            reasons.append((
                "UNKNOWN_CUSTOMER",
                f"customer_id {customer_id!r} does not exist in raw.customers",
            ))

        if account_customer_id is None and account_id:
            reasons.append((
                "UNKNOWN_ACCOUNT",
                f"account_id {account_id!r} does not exist in raw.accounts",
            ))

        return tuple(reasons)


class RejectedRecordWriter:
    """Persist invalid Kafka records into an idempotent quarantine table."""

    def __init__(self, config: PostgresConfig) -> None:
        self.config = config
        self.connection: Any | None = None

    def close(self) -> None:
        if self.connection is not None:
            try:
                self.connection.close()
            finally:
                self.connection = None

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
                application_name="finbank-quality-consumer",
            )
        except Exception as exc:
            record_operational_event(
                "error",
                "database",
                message=str(exc),
                error_class=type(exc).__name__,
                operation="quality_database_connect",
            )
            raise

    def quarantine(
        self,
        *,
        event_id: str | None,
        transaction_id: str | None,
        rejection_code: str,
        rejection_reason: str,
        raw_payload: str,
        kafka_topic: str,
        kafka_partition: int,
        kafka_offset: int,
    ) -> bool:
        if self.connection is None or self.connection.closed:
            self.connect()

        assert self.connection is not None

        try:
            with self.connection.transaction():
                with self.connection.cursor() as cursor:
                    cursor.execute(
                        INSERT_REJECTED_RECORD_SQL,
                        {
                            "event_id": event_id,
                            "transaction_id": transaction_id,
                            "rejection_code": rejection_code,
                            "rejection_reason": rejection_reason,
                            "raw_payload": raw_payload,
                            "kafka_topic": kafka_topic,
                            "kafka_partition": kafka_partition,
                            "kafka_offset": kafka_offset,
                        },
                    )
                    return cursor.fetchone() is not None
        except Exception as exc:
            record_operational_event(
                "error",
                "database",
                message=str(exc),
                error_class=type(exc).__name__,
                operation="quarantine_insert",
            )
            raise
