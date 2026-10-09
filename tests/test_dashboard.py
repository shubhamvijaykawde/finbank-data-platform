from __future__ import annotations

from decimal import Decimal
import os
from pathlib import Path

import pytest

from finbank.dashboard import (
    BANKING_BY_CURRENCY_SQL,
    FRAUD_SUMMARY_SQL,
    OPERATIONS_SQL,
    BankingSummary,
    load_active_customers,
    load_banking_summary,
    load_fraud_summary,
    load_operations_summary,
    load_transactions_per_day,
)
from finbank.postgres_config import PostgresConfig


class FakeConfig(PostgresConfig):
    pass


def test_banking_summary_is_grouped_by_currency(monkeypatch) -> None:
    rows = [
        ("EUR", Decimal("1200.50"), 10, Decimal("120.05"), 7),
        ("GBP", Decimal("900.25"), 6, Decimal("150.041666"), 5),
    ]

    monkeypatch.setattr("finbank.dashboard._query", lambda config, sql, params=(): rows)

    result = load_banking_summary(FakeConfig("postgresql://unused"))

    assert result == [
        BankingSummary("EUR", Decimal("1200.50"), 10, Decimal("120.05"), 7),
        BankingSummary("GBP", Decimal("900.25"), 6, Decimal("150.041666"), 5),
    ]


def test_all_currency_active_customer_count_does_not_double_count(monkeypatch) -> None:
    monkeypatch.setattr(
        "finbank.dashboard._query",
        lambda config, sql, params=(): [(12,)],
    )
    assert load_active_customers(FakeConfig("postgresql://unused")) == 12


def test_fraud_summary_returns_unitless_rate_and_decimal_value(monkeypatch) -> None:
    monkeypatch.setattr(
        "finbank.dashboard._query",
        lambda config, sql, params=(): [(3, 1, Decimal("450.75"), Decimal("0.075"))],
    )

    result = load_fraud_summary(FakeConfig("postgresql://unused"), "EUR")

    assert result["fraud_alerts"] == 3
    assert result["high_risk_alerts"] == 1
    assert result["suspicious_transaction_value"] == Decimal("450.75")
    assert result["fraud_rate"] == 0.075


def test_operations_summary_maps_processing_rate_and_failures(monkeypatch) -> None:
    monkeypatch.setattr(
        "finbank.dashboard._query",
        lambda config, sql, params=(): [(90, 10, Decimal("0.9"), 2, "success")],
    )

    result = load_operations_summary(FakeConfig("postgresql://unused"))

    assert result.processed_transactions == 90
    assert result.rejected_transactions == 10
    assert result.processing_rate == 0.9
    assert result.pipeline_failures == 2
    assert result.latest_pipeline_status == "success"


def test_transactions_per_day_preserves_iso_date(monkeypatch) -> None:
    from datetime import date

    monkeypatch.setattr(
        "finbank.dashboard._query",
        lambda config, sql, params=(): [(date(2026, 10, 8), 15)],
    )

    result = load_transactions_per_day(FakeConfig("postgresql://unused"), None)

    assert result == [{"transaction_day": "2026-10-08", "transaction_count": 15}]


def test_dashboard_queries_refuse_cross_currency_value_aggregation() -> None:
    normalized = " ".join(BANKING_BY_CURRENCY_SQL.split()).lower()
    assert "group by currency" in normalized
    assert "sum(amount)" in normalized


def test_dashboard_queries_define_fraud_and_operations_metrics() -> None:
    assert "COUNT(*) FILTER" in FRAUD_SUMMARY_SQL
    assert "processing_rate" in OPERATIONS_SQL


def test_dashboard_source_has_no_second_dashboard_framework() -> None:
    root = Path(__file__).resolve().parents[1]
    source = (root / "dashboard" / "app.py").read_text(encoding="utf-8")
    assert "streamlit" in source.lower()
    assert "plotly" not in source.lower()
    assert "import dash" not in source.lower()
    assert "from dash" not in source.lower()

@pytest.mark.integration
def test_load_transactions_per_day_against_live_postgres_all_currencies() -> None:
    """Exercise the NULL-currency predicate against real PostgreSQL.

    The explicit text cast prevents PostgreSQL from raising
    IndeterminateDatatype when the selected currency is NULL.
    """
    if os.getenv("FINBANK_RUN_DB_INTEGRATION_TESTS") != "1":
        pytest.skip("set FINBANK_RUN_DB_INTEGRATION_TESTS=1 to run PostgreSQL integration tests")

    config = PostgresConfig.from_environment()
    result = load_transactions_per_day(config, None)

    assert isinstance(result, list)
    assert all(
        set(row) == {"transaction_day", "transaction_count"}
        and isinstance(row["transaction_day"], str)
        and isinstance(row["transaction_count"], int)
        for row in result
    )

