"""Execute the complete live FinBank data path against native Kafka/PostgreSQL."""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import os
from pathlib import Path
import re
import subprocess
import sys

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def _run(command: list[str], *, env: dict[str, str] | None = None) -> str:
    result = subprocess.run(
        command,
        cwd=REPOSITORY_ROOT,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.stdout:
        print(result.stdout, end="")
    if result.stderr:
        print(result.stderr, end="")
    if result.returncode != 0:
        raise RuntimeError(
            f"Command failed with exit code {result.returncode}: {' '.join(command)}"
        )
    return result.stdout + result.stderr


def _rewrite_transaction_ids(source: Path, target: Path, run_id: str) -> int:
    """Create unique transaction IDs so the live test never depends on prior rows."""
    target.parent.mkdir(parents=True, exist_ok=True)
    with source.open("r", newline="", encoding="utf-8") as source_handle:
        reader = csv.DictReader(source_handle)
        if not reader.fieldnames or "transaction_id" not in reader.fieldnames:
            raise ValueError("generated transactions.csv must contain transaction_id")

        with target.open("w", newline="", encoding="utf-8") as target_handle:
            writer = csv.DictWriter(target_handle, fieldnames=reader.fieldnames)
            writer.writeheader()
            count = 0
            for row in reader:
                row["transaction_id"] = f"E2E_{run_id}_{row['transaction_id']}"
                writer.writerow(row)
                count += 1
    return count


def _verify_live_path(
    dsn: str,
    prefix: str,
    expected_count: int,
) -> tuple[int, int, int]:
    import psycopg

    with psycopg.connect(dsn, connect_timeout=5) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM raw.transaction_events WHERE transaction_id LIKE %s",
                (f"{prefix}%",),
            )
            raw_count = int(cursor.fetchone()[0])

            cursor.execute(
                "SELECT COUNT(*) FROM analytics.fact_transactions WHERE transaction_id LIKE %s",
                (f"{prefix}%",),
            )
            fact_count = int(cursor.fetchone()[0])

            cursor.execute(
                "SELECT COUNT(*) FROM analytics.fact_fraud_alerts WHERE transaction_id LIKE %s",
                (f"{prefix}%",),
            )
            fraud_count = int(cursor.fetchone()[0])

    if raw_count != expected_count:
        raise AssertionError(f"raw.transaction_events count={raw_count}; expected {expected_count}")
    if fact_count != expected_count:
        raise AssertionError(f"analytics.fact_transactions count={fact_count}; expected {expected_count}")

    return raw_count, fact_count, fraud_count


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run and verify Generator -> Kafka -> Consumer -> PostgreSQL -> dbt -> Fraud analytics."
    )
    parser.add_argument("--transactions", type=int, default=25)
    parser.add_argument("--seed", type=int, default=4242)
    args = parser.parse_args()

    if args.transactions < 1:
        raise SystemExit("--transactions must be at least 1")

    dsn = os.getenv("POSTGRES_DSN", "").strip()
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").strip()
    if not dsn:
        raise SystemExit("POSTGRES_DSN is required")
    if not bootstrap:
        raise SystemExit("KAFKA_BOOTSTRAP_SERVERS cannot be empty")

    run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
    topic = f"finbank-e2e-{run_id}"
    consumer_group = f"finbank-e2e-group-{run_id}"
    data_dir = REPOSITORY_ROOT / "data" / "e2e" / run_id
    transaction_path = data_dir / "transactions.csv"
    source_transactions = REPOSITORY_ROOT / "data" / "generated" / "transactions.csv"
    prefix = f"E2E_{run_id}_"

    env = os.environ.copy()
    env["KAFKA_BOOTSTRAP_SERVERS"] = bootstrap
    env["KAFKA_TOPIC"] = topic
    env["KAFKA_CONSUMER_GROUP"] = consumer_group

    print("=== FinBank live end-to-end verification ===")
    print("This is not a mocked test: it requires native Kafka, PostgreSQL, and dbt.")

    _run(
        [
            sys.executable,
            "scripts/generate_data.py",
            "--customers",
            "1000",
            "--accounts",
            "1200",
            "--merchants",
            "200",
            "--transactions",
            str(args.transactions),
            "--seed",
            str(args.seed),
            "--output-dir",
            "data/generated",
        ],
        env=env,
    )
    _run([sys.executable, "scripts/init_database.py"], env=env)
    _run([sys.executable, "scripts/load_reference_data.py"], env=env)
    _run(
        [
            sys.executable,
            "scripts/create_kafka_topic.py",
            "--bootstrap-servers",
            bootstrap,
            "--topic",
            topic,
        ],
        env=env,
    )

    generated_count = _rewrite_transaction_ids(
        source_transactions,
        transaction_path,
        run_id,
    )
    if generated_count != args.transactions:
        raise AssertionError(
            f"rewritten transaction count={generated_count}; expected {args.transactions}"
        )

    producer_output = _run(
        [
            sys.executable,
            "scripts/produce_transactions.py",
            "--input",
            str(transaction_path),
            "--bootstrap-servers",
            bootstrap,
            "--topic",
            topic,
        ],
        env=env,
    )
    if not re.search(rf"published={args.transactions}\b", producer_output):
        raise AssertionError("producer output did not report the expected published count")

    consumer_output = _run(
        [
            sys.executable,
            "scripts/consume_transactions_with_quality.py",
            "--bootstrap-servers",
            bootstrap,
            "--topic",
            topic,
            "--group-id",
            consumer_group,
            "--auto-offset-reset",
            "earliest",
            "--max-messages",
            str(args.transactions),
        ],
        env=env,
    )
    if not re.search(rf"inserted={args.transactions}\b", consumer_output):
        raise AssertionError("consumer output did not report the expected inserted count")

    _run([sys.executable, "scripts/run_pipeline.py"], env=env)

    raw_count, fact_count, fraud_count = _verify_live_path(
        dsn,
        prefix,
        args.transactions,
    )

    print(
        "END-TO-END VERIFICATION PASSED: "
        f"generated={generated_count} raw={raw_count} fact={fact_count} fraud_alerts={fraud_count}"
    )
    print(f"Kafka topic used: {topic}")
    print(f"Temporary verification data: {data_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
