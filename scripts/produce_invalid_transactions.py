"""Publish controlled invalid transaction events for Phase 4."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
import sys

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPOSITORY_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

from finbank.kafka_config import KafkaConfig
from finbank.kafka_events import build_transaction_event
from finbank.kafka_producer import create_producer
from finbank.logging_config import configure_logging

CORRUPTION_TYPES = (
    "missing_customer_id",
    "missing_account_id",
    "negative_amount",
    "invalid_currency",
    "unknown_merchant",
    "duplicate_transaction",
    "future_timestamp",
    "invalid_status",
)


def mutate_transaction(row: dict[str, str], corruption_type: str) -> dict[str, str]:
    mutated = dict(row)

    if corruption_type == "missing_customer_id":
        mutated["customer_id"] = ""
    elif corruption_type == "missing_account_id":
        mutated["account_id"] = ""
    elif corruption_type == "negative_amount":
        mutated["amount"] = "-100.00"
    elif corruption_type == "invalid_currency":
        mutated["currency"] = "XXX"
    elif corruption_type == "unknown_merchant":
        mutated["merchant_id"] = "MER999999"
    elif corruption_type == "future_timestamp":
        mutated["timestamp"] = "2099-01-01T00:00:00+00:00"
    elif corruption_type == "invalid_status":
        mutated["status"] = "reversed"
    elif corruption_type == "duplicate_transaction":
        pass
    else:
        raise ValueError(f"Unsupported corruption type: {corruption_type}")

    return mutated


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Publish deterministic Phase 4 invalid FinBank transaction events."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("data/generated/transactions.csv"),
    )
    parser.add_argument("--bootstrap-servers", default=None)
    parser.add_argument("--topic", default=None)
    parser.add_argument(
        "--types",
        nargs="+",
        choices=CORRUPTION_TYPES,
        default=list(CORRUPTION_TYPES),
    )
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    if not args.input.exists():
        raise SystemExit(f"Transaction CSV not found: {args.input}")

    config = KafkaConfig.from_environment()
    config = KafkaConfig(
        bootstrap_servers=(
            config.bootstrap_servers if args.bootstrap_servers is None else args.bootstrap_servers
        ),
        topic=config.topic if args.topic is None else args.topic,
        consumer_group=config.consumer_group,
    )

    configure_logging(args.log_level)
    producer = create_producer(config)

    with args.input.open("r", newline="", encoding="utf-8") as handle:
        row = next(csv.DictReader(handle), None)

    if row is None:
        raise SystemExit("Transaction CSV contains no rows")

    published = 0
    try:
        for index, corruption_type in enumerate(args.types, start=1):
            mutated = mutate_transaction(row, corruption_type)
            event_id = (
                row["transaction_id"]
                if corruption_type != "duplicate_transaction"
                else f"DUP-EVENT-{row['transaction_id']}"
            )
            event = build_transaction_event(mutated, event_id=event_id)
            producer.send(
                config.topic,
                key=mutated["transaction_id"],
                value=event,
            ).get(timeout=15)
            published += 1
            print(
                f"Published invalid event {index}: "
                f"type={corruption_type} event_id={event_id} "
                f"transaction_id={mutated['transaction_id']}"
            )
    finally:
        producer.flush()
        producer.close()

    print(f"Published {published} invalid events to topic '{config.topic}'.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
