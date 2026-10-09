"""Collect and persist Phase 9 operational metrics."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPOSITORY_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

from finbank.monitoring import collect_metrics, persist_metrics_snapshot, write_metrics_json
from finbank.postgres_config import PostgresConfig


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Collect lightweight FinBank operational metrics."
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/metrics/latest_metrics.json"),
    )
    args = parser.parse_args()

    try:
        config = PostgresConfig.from_environment()
        snapshot = collect_metrics(config)
        persist_metrics_snapshot(config, snapshot)
        output_path = write_metrics_json(snapshot, args.output)
    except Exception as exc:  # noqa: BLE001 - command-line boundary
        print(f"Metrics collection error: {exc}")
        return 1

    print(
        "FinBank metrics collected: "
        f"processed={snapshot.transactions_processed} "
        f"rejected={snapshot.transactions_rejected} "
        f"fraud_alerts={snapshot.fraud_alerts} "
        f"consumer_errors={snapshot.consumer_errors} "
        f"database_errors={snapshot.database_errors} "
        f"output={output_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
