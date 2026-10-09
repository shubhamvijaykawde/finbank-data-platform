"""Dashboard data access and business-metric calculations for FinBank."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Sequence

from finbank.postgres_config import PostgresConfig


@dataclass(frozen=True)
class BankingSummary:
    """Completed-transaction banking metrics for one currency."""

    currency: str
    transaction_value: Decimal
    transaction_count: int
    average_transaction: Decimal
    active_customers: int


@dataclass(frozen=True)
class OperationsSummary:
    """Operational throughput and pipeline-health metrics."""

    processed_transactions: int
    rejected_transactions: int
    processing_rate: float
    pipeline_failures: int
    latest_pipeline_status: str


BANKING_BY_CURRENCY_SQL = """
SELECT
    currency,
    COALESCE(SUM(amount), 0) AS transaction_value,
    COUNT(*) AS transaction_count,
    COALESCE(AVG(amount), 0) AS average_transaction,
    COUNT(DISTINCT customer_sk) AS active_customers
FROM analytics.fact_transactions
WHERE status = 'completed'
GROUP BY currency
ORDER BY currency
"""

ACTIVE_CUSTOMERS_SQL = """
SELECT COUNT(DISTINCT customer_sk)
FROM analytics.fact_transactions
WHERE status = 'completed'
"""

TRANSACTIONS_PER_DAY_SQL = """
SELECT
    transaction_timestamp::date AS transaction_day,
    COUNT(*) AS transaction_count
FROM analytics.fact_transactions
WHERE status = 'completed'
  AND (%s::text IS NULL OR currency = %s::text)
GROUP BY transaction_timestamp::date
ORDER BY transaction_day
"""

FRAUD_SUMMARY_SQL = """
SELECT
    COUNT(*) AS fraud_alerts,
    COUNT(*) FILTER (WHERE severity = 'HIGH') AS high_risk_alerts,
    COALESCE(SUM(amount), 0) AS suspicious_transaction_value,
    ROUND(
        COUNT(*)::numeric / NULLIF(
            (SELECT COUNT(*)
             FROM analytics.fact_transactions
             WHERE (%s::text IS NULL OR currency = %s::text)),
            0
        ),
        6
    ) AS fraud_rate
FROM analytics.fact_fraud_alerts
WHERE (%s::text IS NULL OR currency = %s::text)
"""

FRAUD_BY_COUNTRY_SQL = """
SELECT
    transaction_country AS country,
    COUNT(*) AS fraud_alerts
FROM analytics.fact_fraud_alerts
WHERE (%s::text IS NULL OR currency = %s::text)
GROUP BY transaction_country
ORDER BY fraud_alerts DESC, country
"""

FRAUD_BY_CATEGORY_SQL = """
SELECT
    merchant_category,
    COUNT(*) AS fraud_alerts
FROM analytics.fact_fraud_alerts
WHERE (%s::text IS NULL OR currency = %s::text)
GROUP BY merchant_category
ORDER BY fraud_alerts DESC, merchant_category
"""

OPERATIONS_SQL = """
WITH counts AS (
    SELECT
        (SELECT COUNT(*) FROM raw.transaction_events) AS processed_transactions,
        (SELECT COUNT(*) FROM raw.rejected_records) AS rejected_transactions,
        (SELECT COUNT(*)
         FROM monitoring.metric_snapshots
         WHERE pipeline_status = 'failed') AS pipeline_failures,
        (SELECT pipeline_status
         FROM monitoring.metric_snapshots
         ORDER BY captured_at DESC
         LIMIT 1) AS latest_pipeline_status
)
SELECT
    processed_transactions,
    rejected_transactions,
    ROUND(
        processed_transactions::numeric
        / NULLIF(processed_transactions + rejected_transactions, 0),
        6
    ) AS processing_rate,
    pipeline_failures,
    COALESCE(latest_pipeline_status, 'unknown') AS latest_pipeline_status
FROM counts
"""


def _query(config: PostgresConfig, sql: str, params: Sequence[Any] = ()) -> list[tuple[Any, ...]]:
    """Execute one read-only dashboard query and return its rows."""
    import psycopg

    with psycopg.connect(
        config.dsn,
        connect_timeout=config.connect_timeout,
        application_name="finbank-streamlit-dashboard",
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql, params)
            return list(cursor.fetchall())


def _currency_param(currency: str | None) -> tuple[str | None, str | None]:
    """Return a parameter tuple compatible with the optional currency predicates."""
    normalized = None if currency in {None, "All currencies"} else currency
    return normalized, normalized


def load_banking_summary(config: PostgresConfig) -> list[BankingSummary]:
    rows = _query(config, BANKING_BY_CURRENCY_SQL)
    return [
        BankingSummary(
            currency=str(row[0]),
            transaction_value=Decimal(row[1]),
            transaction_count=int(row[2]),
            average_transaction=Decimal(row[3]),
            active_customers=int(row[4]),
        )
        for row in rows
    ]


def load_active_customers(config: PostgresConfig) -> int:
    """Count distinct customers with at least one completed transaction."""
    return int(_query(config, ACTIVE_CUSTOMERS_SQL)[0][0])


def load_transactions_per_day(
    config: PostgresConfig,
    currency: str | None,
) -> list[dict[str, Any]]:
    params = _currency_param(currency)
    rows = _query(config, TRANSACTIONS_PER_DAY_SQL, params)
    return [
        {"transaction_day": row[0].isoformat(), "transaction_count": int(row[1])}
        for row in rows
    ]


def load_fraud_summary(config: PostgresConfig, currency: str | None) -> dict[str, Any]:
    param1, param2 = _currency_param(currency)
    rows = _query(
        config,
        FRAUD_SUMMARY_SQL,
        (param1, param2, param1, param2),
    )
    row = rows[0]
    return {
        "fraud_alerts": int(row[0]),
        "high_risk_alerts": int(row[1]),
        "suspicious_transaction_value": Decimal(row[2]),
        "fraud_rate": float(row[3] or 0),
    }


def load_fraud_by_country(config: PostgresConfig, currency: str | None) -> list[dict[str, Any]]:
    params = _currency_param(currency)
    rows = _query(config, FRAUD_BY_COUNTRY_SQL, params)
    return [{"country": str(row[0]), "fraud_alerts": int(row[1])} for row in rows]


def load_fraud_by_category(config: PostgresConfig, currency: str | None) -> list[dict[str, Any]]:
    params = _currency_param(currency)
    rows = _query(config, FRAUD_BY_CATEGORY_SQL, params)
    return [
        {"merchant_category": str(row[0]), "fraud_alerts": int(row[1])}
        for row in rows
    ]


def load_operations_summary(config: PostgresConfig) -> OperationsSummary:
    row = _query(config, OPERATIONS_SQL)[0]
    return OperationsSummary(
        processed_transactions=int(row[0]),
        rejected_transactions=int(row[1]),
        processing_rate=float(row[2] or 0),
        pipeline_failures=int(row[3]),
        latest_pipeline_status=str(row[4]),
    )
