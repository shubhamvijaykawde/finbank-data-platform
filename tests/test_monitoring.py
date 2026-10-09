from __future__ import annotations

from pathlib import Path

from datetime import datetime, timezone
import re
import sys
import types

from finbank.monitoring import MetricsSnapshot, _snapshot_db_params, collect_metrics, write_metrics_json
from finbank.operational_events import read_operational_events, record_operational_event


def test_operational_events_round_trip(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"

    record_operational_event(
        "ingestion_run",
        "consumer",
        log_path=path,
        mode="test",
        processed=10,
        inserted=8,
        duplicates=1,
        rejected=1,
        elapsed_seconds=2.0,
    )

    events = read_operational_events(path)

    assert len(events) == 1
    assert events[0]["event_type"] == "ingestion_run"
    assert events[0]["processed"] == 10


def test_metrics_json_contains_all_required_fields(tmp_path: Path) -> None:
    snapshot = MetricsSnapshot(
        transactions_generated=100,
        transactions_processed=90,
        transactions_rejected=10,
        transactions_per_minute=300.0,
        fraud_alerts=2,
        high_risk_transactions=1,
        consumer_errors=0,
        database_errors=0,
        pipeline_execution_time_seconds=8.5,
        pipeline_status="success",
        captured_at=datetime(2026, 10, 8, tzinfo=timezone.utc),
        throughput_window_seconds=18.0,
        error_interval_start_utc=None,
    )

    output = write_metrics_json(snapshot, tmp_path / "metrics.json")
    text = output.read_text(encoding="utf-8")

    assert "captured_at" in text
    assert "captured_at_utc" not in text

    for field in (
        "transactions_generated",
        "transactions_processed",
        "transactions_rejected",
        "transactions_per_minute",
        "fraud_alerts",
        "high_risk_transactions",
        "consumer_errors",
        "database_errors",
        "pipeline_execution_time_seconds",
    ):
        assert f'"{field}"' in text


def test_phase9_artifacts_exist() -> None:
    root = Path(__file__).resolve().parents[1]

    for relative_path in (
        "src/finbank/monitoring.py",
        "src/finbank/operational_events.py",
        "scripts/collect_metrics.py",
        "sql/008_create_monitoring.sql",
        "sql/009_monitoring_verification.sql",
        "docs/monitoring-phase9.md",
    ):
        assert (root / relative_path).exists()


def test_verified_dependency_pins_remain_unchanged() -> None:
    root = Path(__file__).resolve().parents[1]
    requirements = (root / "requirements.txt").read_text(encoding="utf-8")
    assert "kafka-python==3.0.11" in requirements
    assert "psycopg[binary]==3.3.6" in requirements
    assert "dbt-postgres==1.11.0" in requirements

    project = (root / "dbt" / "dbt_project.yml").read_text(encoding="utf-8")
    assert 'require-dbt-version: ">=1.12.0,<2.0.0"' in project


def test_no_new_runtime_dependency_is_required() -> None:
    requirements = (
        Path(__file__).resolve().parents[1] / "requirements.txt"
    ).read_text(encoding="utf-8")
    assert "prometheus" not in requirements.lower()
    assert "grafana" not in requirements.lower()
    assert "elasticsearch" not in requirements.lower()


def test_phase9_docs_are_cmd_oriented() -> None:
    root = Path(__file__).resolve().parents[1]
    text = (root / "docs" / "monitoring-phase9.md").read_text(encoding="utf-8").lower()
    assert "$env:" not in text
    assert "powershell" not in text


def test_init_database_includes_monitoring_sql() -> None:
    root = Path(__file__).resolve().parents[1]
    text = (root / "scripts" / "init_database.py").read_text(encoding="utf-8")
    assert '"sql" / "008_create_monitoring.sql"' in text


def test_monitoring_insert_parameters_match_sql_placeholders() -> None:
    root = Path(__file__).resolve().parents[1]
    sql = (root / "src" / "finbank" / "monitoring.py").read_text(encoding="utf-8")
    placeholders = set(re.findall(r"%\(([^)]+)\)s", sql))

    snapshot = MetricsSnapshot(
        transactions_generated=1,
        transactions_processed=1,
        transactions_rejected=0,
        transactions_per_minute=1.0,
        fraud_alerts=0,
        high_risk_transactions=0,
        consumer_errors=0,
        database_errors=0,
        pipeline_execution_time_seconds=1.0,
        pipeline_status="success",
        captured_at=datetime.now(timezone.utc),
        throughput_window_seconds=60.0,
        error_interval_start_utc=None,
    )
    params = _snapshot_db_params(snapshot)

    assert placeholders == set(params)
    assert isinstance(params["captured_at"], datetime)
    assert params["captured_at"].tzinfo is not None



def test_collect_metrics_preserves_previous_capture_as_datetime(tmp_path: Path, monkeypatch) -> None:
    """Regression: a second collection must accept the prior TIMESTAMPTZ value."""
    previous_capture = datetime(2026, 10, 8, 23, 30, tzinfo=timezone.utc)

    class FakeCursor:
        def __init__(self) -> None:
            self.last_query = ""

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, query, params=None) -> None:
            self.last_query = query

        def fetchone(self):
            if "SELECT captured_at" in self.last_query:
                return (previous_capture,)
            return (0,)

    class FakeConnection:
        def __init__(self) -> None:
            self.cursor_obj = FakeCursor()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return self.cursor_obj

    fake_psycopg = types.SimpleNamespace(connect=lambda *args, **kwargs: FakeConnection())
    monkeypatch.setitem(sys.modules, "psycopg", fake_psycopg)

    generated = tmp_path / "transactions.csv"
    generated.write_text("transaction_id\nTXN00000001\n", encoding="utf-8")
    pipeline = tmp_path / "pipeline.json"
    pipeline.write_text('{"status":"success","elapsed_seconds":1.0}', encoding="utf-8")
    events = tmp_path / "events.jsonl"
    events.write_text("", encoding="utf-8")

    class FakeConfig:
        dsn = "fake"
        connect_timeout = 1

    snapshot = collect_metrics(
        FakeConfig(),
        generated_transactions_path=generated,
        pipeline_run_path=pipeline,
        event_log_path=events,
    )

    assert snapshot.error_interval_start_utc == previous_capture
    assert isinstance(snapshot.error_interval_start_utc, datetime)
    assert snapshot.error_interval_start_utc.tzinfo is not None


