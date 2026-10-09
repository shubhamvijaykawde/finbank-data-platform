"""Generate a lightweight FinBank analytical snapshot."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPOSITORY_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

from finbank.analytics_report import generate_analytics_report
from finbank.postgres_config import PostgresConfig


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate a JSON analytical snapshot from FinBank marts."
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/analytics/latest_report.json"),
    )
    args = parser.parse_args()

    output_path = (
        REPOSITORY_ROOT / args.output
        if not args.output.is_absolute()
        else args.output
    )

    try:
        config = PostgresConfig.from_environment()
        report = generate_analytics_report(config, output_path)
    except Exception as exc:  # noqa: BLE001 - command-line boundary
        print(f"Analytics generation error: {exc}")
        return 1

    print(
        "Analytics report generated: "
        f"completed_transactions={report['completed_transactions']['transaction_count']} "
        f"fraud_alerts={report['fraud']['fraud_alerts']} "
        f"rejected_records={report['rejected_records']} "
        f"output={output_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
