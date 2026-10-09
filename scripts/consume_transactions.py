"""Repository entry point for the Phase 2 Kafka consumer."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPOSITORY_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

from finbank.kafka_config import KafkaConfig
from finbank.kafka_consumer import consume_transactions
from finbank.logging_config import configure_logging


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Consume FinBank transaction events from Kafka."
    )
    parser.add_argument("--bootstrap-servers", default=None)
    parser.add_argument("--topic", default=None)
    parser.add_argument("--group-id", default=None)
    parser.add_argument("--max-messages", type=int, default=None)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    if args.max_messages is not None and args.max_messages < 1:
        raise SystemExit("--max-messages must be at least 1")

    config = KafkaConfig.from_environment()
    config = KafkaConfig(
        bootstrap_servers=(
            config.bootstrap_servers
            if args.bootstrap_servers is None
            else args.bootstrap_servers
        ),
        topic=config.topic if args.topic is None else args.topic,
        consumer_group=(
            config.consumer_group
            if args.group_id is None
            else args.group_id
        ),
    )

    configure_logging(args.log_level)

    try:
        consume_transactions(
            config=config,
            max_messages=args.max_messages,
        )
    except (RuntimeError, ValueError, UnicodeDecodeError) as exc:
        print(f"Consumer error: {exc}")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
