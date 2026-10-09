"""Generate a lightweight analytical snapshot from FinBank marts."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

from finbank.postgres_config import PostgresConfig


QUERY_TOTALS = """
SELECT
    COUNT(*) AS transaction_count,
    COUNT(DISTINCT customer_sk) AS active_customers
FROM analytics.fact_transactions
WHERE status = 'completed'
"""

QUERY_VALUE_BY_CURRENCY = """
SELECT
    currency,
    COUNT(*) AS transaction_count,
    SUM(amount) AS completed_value
FROM analytics.fact_transactions
WHERE status = 'completed'
GROUP BY currency
ORDER BY currency
"""

QUERY_FRAUD = """
SELECT
    COUNT(*) AS fraud_alerts,
    COUNT(*) FILTER (WHERE severity = 'HIGH') AS high_risk_alerts,
    COALESCE(SUM(amount), 0) AS suspicious_transaction_value
FROM analytics.fact_fraud_alerts
"""

QUERY_REJECTIONS = """
SELECT COUNT(*) AS rejected_records
FROM raw.rejected_records
"""


def _decimal_to_string(value: Any) -> str:
    return format(value, "f") if value is not None else "0.00"


def generate_analytics_report(
    config: PostgresConfig,
    output_path: Path,
) -> dict[str, Any]:
    """Create and atomically replace one JSON analytical snapshot."""
    import psycopg

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with psycopg.connect(
        config.dsn,
        connect_timeout=config.connect_timeout,
        application_name="finbank-analytics-report",
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(QUERY_TOTALS)
            total_row = cursor.fetchone()

            cursor.execute(QUERY_VALUE_BY_CURRENCY)
            currency_rows = cursor.fetchall()

            cursor.execute(QUERY_FRAUD)
            fraud_row = cursor.fetchone()

            cursor.execute(QUERY_REJECTIONS)
            rejection_row = cursor.fetchone()

    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "completed_transactions": {
            "transaction_count": total_row[0],
            "active_customers": total_row[1],
        },
        "completed_value_by_currency": [
            {
                "currency": row[0],
                "transaction_count": row[1],
                "completed_value": _decimal_to_string(row[2]),
            }
            for row in currency_rows
        ],
        "fraud": {
            "fraud_alerts": fraud_row[0],
            "high_risk_alerts": fraud_row[1],
            "suspicious_transaction_value": _decimal_to_string(fraud_row[2]),
        },
        "rejected_records": rejection_row[0],
    }

    with NamedTemporaryFile(
        "w",
        encoding="utf-8",
        dir=output_path.parent,
        prefix=f".{output_path.stem}-",
        suffix=".tmp",
        delete=False,
    ) as handle:
        temporary_path = Path(handle.name)
        json.dump(report, handle, indent=2, sort_keys=True)
        handle.write("\n")

    temporary_path.replace(output_path)
    return report
