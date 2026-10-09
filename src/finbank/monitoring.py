"""Lightweight operational metrics collection for FinBank Phase 9."""

from __future__ import annotations

import csv
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

from finbank.operational_events import read_operational_events
from finbank.postgres_config import PostgresConfig

DEFAULT_METRICS_OUTPUT = Path("data/metrics/latest_metrics.json")
DEFAULT_GENERATED_TRANSACTIONS = Path("data/generated/transactions.csv")


@dataclass(frozen=True)
class MetricsSnapshot:
    transactions_generated: int
    transactions_processed: int
    transactions_rejected: int
    transactions_per_minute: float
    fraud_alerts: int
    high_risk_transactions: int
    consumer_errors: int
    database_errors: int
    pipeline_execution_time_seconds: float | None
    pipeline_status: str
    captured_at: datetime
    throughput_window_seconds: float
    error_interval_start_utc: datetime | None


COUNT_QUERY = {
    "transactions_processed": "SELECT COUNT(*) FROM raw.transaction_events",
    "transactions_rejected": "SELECT COUNT(*) FROM raw.rejected_records",
    "fraud_alerts": "SELECT COUNT(*) FROM analytics.fact_fraud_alerts",
    "high_risk_transactions": "SELECT COUNT(*) FROM analytics.fact_fraud_alerts WHERE severity = 'HIGH'",
}


def count_generated_transactions(csv_path: Path = DEFAULT_GENERATED_TRANSACTIONS) -> int:
    """Count transaction rows in the Phase 1 generated source CSV."""
    if not csv_path.exists():
        raise FileNotFoundError(f"Generated transaction file not found: {csv_path}")
    with csv_path.open("r", newline="", encoding="utf-8") as handle:
        return sum(1 for _ in csv.DictReader(handle))


def _fetch_database_counts(config: PostgresConfig) -> dict[str, int]:
    import psycopg

    result: dict[str, int] = {}
    with psycopg.connect(
        config.dsn,
        connect_timeout=config.connect_timeout,
        application_name="finbank-monitoring",
    ) as connection:
        with connection.cursor() as cursor:
            for name, query in COUNT_QUERY.items():
                cursor.execute(query)
                row = cursor.fetchone()
                result[name] = int(row[0])
    return result


def _read_pipeline_run(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _parse_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _events_since(events: list[dict[str, Any]], start: datetime | None) -> list[dict[str, Any]]:
    if start is None:
        return events
    selected: list[dict[str, Any]] = []
    for event in events:
        timestamp = _parse_timestamp(event.get("event_timestamp"))
        if timestamp is not None and timestamp > start:
            selected.append(event)
    return selected


def _throughput_metrics(events: list[dict[str, Any]]) -> tuple[float, float]:
    """Return (transactions_per_minute, observed_seconds) from ingestion events."""
    ingestion_events = [
        event
        for event in events
        if event.get("event_type") == "ingestion_run"
    ]
    processed = sum(int(event.get("processed", 0)) for event in ingestion_events)
    elapsed = sum(float(event.get("elapsed_seconds", 0.0)) for event in ingestion_events)

    if processed <= 0 or elapsed <= 0:
        return 0.0, max(elapsed, 0.0)

    return processed / (elapsed / 60.0), elapsed


def collect_metrics(
    config: PostgresConfig,
    *,
    generated_transactions_path: Path = DEFAULT_GENERATED_TRANSACTIONS,
    pipeline_run_path: Path = Path("data/metrics/latest_pipeline_run.json"),
    event_log_path: Path = Path("data/metrics/operational_events.jsonl"),
) -> MetricsSnapshot:
    """Collect a point-in-time operational snapshot from existing state."""
    generated = count_generated_transactions(generated_transactions_path)
    database_counts = _fetch_database_counts(config)

    pipeline_path = (
        pipeline_run_path
        if pipeline_run_path.is_absolute()
        else Path(__file__).resolve().parents[2] / pipeline_run_path
    )
    pipeline_run = _read_pipeline_run(pipeline_path)
    captured_at = datetime.now(timezone.utc)

    previous_capture: datetime | None = None
    # Querying the previous snapshot is intentionally best-effort. The first
    # snapshot counts all existing events; later snapshots count interval errors.
    try:
        import psycopg
        with psycopg.connect(
            config.dsn,
            connect_timeout=config.connect_timeout,
            application_name="finbank-monitoring-history",
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT captured_at FROM monitoring.metric_snapshots ORDER BY captured_at DESC LIMIT 1"
                )
                row = cursor.fetchone()
                if row and row[0] is not None:
                    previous_capture = _parse_timestamp(row[0].isoformat())
    except Exception:
        previous_capture = None

    events = read_operational_events(event_log_path)
    interval_events = _events_since(events, previous_capture)
    consumer_errors = sum(
        1
        for event in interval_events
        if event.get("event_type") == "error"
        and event.get("component") == "consumer"
    )
    database_errors = sum(
        1
        for event in interval_events
        if event.get("event_type") == "error"
        and event.get("component") == "database"
    )

    tpm, throughput_seconds = _throughput_metrics(interval_events)

    pipeline_seconds = pipeline_run.get("elapsed_seconds")
    if pipeline_seconds is not None:
        pipeline_seconds = float(pipeline_seconds)

    return MetricsSnapshot(
        transactions_generated=generated,
        transactions_processed=database_counts["transactions_processed"],
        transactions_rejected=database_counts["transactions_rejected"],
        transactions_per_minute=round(tpm, 4),
        fraud_alerts=database_counts["fraud_alerts"],
        high_risk_transactions=database_counts["high_risk_transactions"],
        consumer_errors=consumer_errors,
        database_errors=database_errors,
        pipeline_execution_time_seconds=pipeline_seconds,
        pipeline_status=str(pipeline_run.get("status", "unknown")),
        captured_at=captured_at,
        throughput_window_seconds=round(throughput_seconds, 4),
        error_interval_start_utc=previous_capture,
    )


def _snapshot_db_params(snapshot: MetricsSnapshot) -> dict[str, Any]:
    """Build the exact parameter mapping expected by the monitoring INSERT.

    Keep this explicit rather than passing ``asdict(snapshot)`` so SQL
    parameter names cannot silently drift from the dataclass field names.
    Timestamps are passed as timezone-aware ``datetime`` objects so psycopg
    binds them directly to PostgreSQL TIMESTAMPTZ columns.
    """
    return {
        "captured_at": snapshot.captured_at,
        "transactions_generated": snapshot.transactions_generated,
        "transactions_processed": snapshot.transactions_processed,
        "transactions_rejected": snapshot.transactions_rejected,
        "transactions_per_minute": snapshot.transactions_per_minute,
        "fraud_alerts": snapshot.fraud_alerts,
        "high_risk_transactions": snapshot.high_risk_transactions,
        "consumer_errors": snapshot.consumer_errors,
        "database_errors": snapshot.database_errors,
        "pipeline_execution_time_seconds": snapshot.pipeline_execution_time_seconds,
        "pipeline_status": snapshot.pipeline_status,
        "throughput_window_seconds": snapshot.throughput_window_seconds,
        "error_interval_start_utc": snapshot.error_interval_start_utc,
    }


def persist_metrics_snapshot(
    config: PostgresConfig,
    snapshot: MetricsSnapshot,
) -> None:
    """Persist one monitoring snapshot without altering analytical models."""
    import psycopg

    with psycopg.connect(
        config.dsn,
        connect_timeout=config.connect_timeout,
        application_name="finbank-monitoring-write",
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO monitoring.metric_snapshots (
                    captured_at,
                    transactions_generated,
                    transactions_processed,
                    transactions_rejected,
                    transactions_per_minute,
                    fraud_alerts,
                    high_risk_transactions,
                    consumer_errors,
                    database_errors,
                    pipeline_execution_time_seconds,
                    pipeline_status,
                    throughput_window_seconds,
                    error_interval_start_utc
                ) VALUES (
                    %(captured_at)s,
                    %(transactions_generated)s,
                    %(transactions_processed)s,
                    %(transactions_rejected)s,
                    %(transactions_per_minute)s,
                    %(fraud_alerts)s,
                    %(high_risk_transactions)s,
                    %(consumer_errors)s,
                    %(database_errors)s,
                    %(pipeline_execution_time_seconds)s,
                    %(pipeline_status)s,
                    %(throughput_window_seconds)s,
                    %(error_interval_start_utc)s
                )
                """,
                _snapshot_db_params(snapshot),
            )


def write_metrics_json(snapshot: MetricsSnapshot, output_path: Path = DEFAULT_METRICS_OUTPUT) -> Path:
    """Atomically replace the latest JSON metrics snapshot."""
    if not output_path.is_absolute():
        output_path = Path(__file__).resolve().parents[2] / output_path
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with NamedTemporaryFile(
        "w",
        encoding="utf-8",
        dir=output_path.parent,
        prefix=f".{output_path.stem}-",
        suffix=".tmp",
        delete=False,
    ) as handle:
        temporary_path = Path(handle.name)
        payload = asdict(snapshot)
        payload["captured_at"] = snapshot.captured_at.isoformat()
        if snapshot.error_interval_start_utc is not None:
            payload["error_interval_start_utc"] = snapshot.error_interval_start_utc.isoformat()
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")

    temporary_path.replace(output_path)
    return output_path
