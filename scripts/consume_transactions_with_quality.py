"""Repository entry point for Phase 4 quality-aware Kafka ingestion."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPOSITORY_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

from finbank.kafka_config import KafkaConfig
from finbank.logging_config import configure_logging
from finbank.postgres_config import PostgresConfig
from finbank.quality_ingestion import consume_with_quality


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Consume FinBank Kafka events with Phase 4 data-quality quarantine."
    )
    parser.add_argument("--bootstrap-servers", default=None)
    parser.add_argument("--topic", default=None)
    parser.add_argument("--group-id", default=None)
    parser.add_argument("--max-messages", type=int, default=None)
    parser.add_argument(
        "--auto-offset-reset",
        choices=("earliest", "latest"),
        default="latest",
        help="Where a new consumer group starts. Default: latest for Phase 4 demos.",
    )
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    if args.max_messages is not None and args.max_messages < 1:
        raise SystemExit("--max-messages must be at least 1")

    kafka = KafkaConfig.from_environment()
    kafka = KafkaConfig(
        bootstrap_servers=kafka.bootstrap_servers if args.bootstrap_servers is None else args.bootstrap_servers,
        topic=kafka.topic if args.topic is None else args.topic,
        consumer_group=kafka.consumer_group if args.group_id is None else args.group_id,
    )
    postgres = PostgresConfig.from_environment()
    configure_logging(args.log_level)

    try:
        inserted, duplicates, rejected = consume_with_quality(
            kafka_config=kafka,
            postgres_config=postgres,
            max_messages=args.max_messages,
            auto_offset_reset=args.auto_offset_reset,
        )
    except (RuntimeError, ValueError, UnicodeDecodeError) as exc:
        print(f"Quality consumer error: {exc}")
        return 1

    print(
        "Phase 4 ingestion complete: "
        f"inserted={inserted} duplicates={duplicates} rejected={rejected}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
