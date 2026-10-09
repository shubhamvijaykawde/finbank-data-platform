from __future__ import annotations

import os

import pytest

from finbank.postgres import INSERT_TRANSACTION_EVENT_SQL, _parse_timestamp
from finbank.postgres_config import PostgresConfig


def test_postgres_config_requires_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("POSTGRES_DSN", raising=False)

    with pytest.raises(ValueError, match="POSTGRES_DSN is required"):
        PostgresConfig.from_environment()


def test_postgres_config_reads_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "POSTGRES_DSN",
        "postgresql://finbank:secret@localhost:5432/finbank",
    )
    monkeypatch.setenv("POSTGRES_CONNECT_TIMEOUT", "7")
    monkeypatch.setenv("POSTGRES_RETRY_ATTEMPTS", "4")
    monkeypatch.setenv("POSTGRES_RETRY_BACKOFF_SECONDS", "0.5")

    config = PostgresConfig.from_environment()

    assert config.dsn == "postgresql://finbank:secret@localhost:5432/finbank"
    assert config.connect_timeout == 7
    assert config.retry_attempts == 4
    assert config.retry_backoff_seconds == 0.5


def test_postgres_config_rejects_invalid_retry_settings() -> None:
    with pytest.raises(ValueError):
        PostgresConfig(
            dsn="postgresql://localhost/finbank",
            retry_attempts=0,
        ).validate()

    with pytest.raises(ValueError):
        PostgresConfig(
            dsn="postgresql://localhost/finbank",
            retry_backoff_seconds=-1,
        ).validate()


def test_timestamp_parser_preserves_timezone() -> None:
    parsed = _parse_timestamp("2025-12-31T12:00:00+00:00")

    assert parsed.isoformat() == "2025-12-31T12:00:00+00:00"
    assert parsed.tzinfo is not None


def test_raw_insert_is_idempotent() -> None:
    normalized = " ".join(INSERT_TRANSACTION_EVENT_SQL.split()).upper()

    assert "ON CONFLICT DO NOTHING" in normalized
    assert "RETURNING EVENT_ID" in normalized
    assert "PAYLOAD" in normalized
    assert "KAFKA_TOPIC" in normalized
    assert "KAFKA_PARTITION" in normalized
    assert "KAFKA_OFFSET" in normalized


def test_raw_insert_contains_business_relationship_columns() -> None:
    normalized = " ".join(INSERT_TRANSACTION_EVENT_SQL.split()).lower()

    for column in (
        "customer_id",
        "account_id",
        "merchant_id",
        "transaction_id",
    ):
        assert column in normalized
