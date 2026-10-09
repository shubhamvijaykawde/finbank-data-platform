"""Repository entry point for the Phase 2 Kafka producer."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPOSITORY_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

from finbank.kafka_config import KafkaConfig
from finbank.kafka_producer import publish_transactions
from finbank.logging_config import configure_logging


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Publish FinBank transaction CSV rows to Kafka."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("data/generated/transactions.csv"),
    )
    parser.add_argument("--bootstrap-servers", default=None)
    parser.add_argument("--topic", default=None)
    parser.add_argument("--max-records", type=int, default=None)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    if args.max_records is not None and args.max_records < 1:
        raise SystemExit("--max-records must be at least 1")

    config = KafkaConfig.from_environment()
    config = KafkaConfig(
        bootstrap_servers=(
            config.bootstrap_servers
            if args.bootstrap_servers is None
            else args.bootstrap_servers
        ),
        topic=config.topic if args.topic is None else args.topic,
        consumer_group=config.consumer_group,
    )

    configure_logging(args.log_level)

    try:
        publish_transactions(
            input_path=args.input,
            config=config,
            max_records=args.max_records,
        )
    except (FileNotFoundError, RuntimeError, ValueError) as exc:
        print(f"Producer error: {exc}")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
