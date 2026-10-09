"""Consume FinBank Kafka transactions into PostgreSQL raw storage."""

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
from finbank.postgres_ingestion import consume_to_postgres


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Consume FinBank Kafka events and persist them to PostgreSQL."
    )
    parser.add_argument("--bootstrap-servers", default=None)
    parser.add_argument("--topic", default=None)
    parser.add_argument("--group-id", default=None)
    parser.add_argument("--max-messages", type=int, default=None)
    parser.add_argument("--fail-after-db-write", action="store_true")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    if args.max_messages is not None and args.max_messages < 1:
        raise SystemExit("--max-messages must be at least 1")

    kafka_base = KafkaConfig.from_environment()
    kafka_config = KafkaConfig(
        bootstrap_servers=(
            kafka_base.bootstrap_servers
            if args.bootstrap_servers is None
            else args.bootstrap_servers
        ),
        topic=kafka_base.topic if args.topic is None else args.topic,
        consumer_group=(
            kafka_base.consumer_group
            if args.group_id is None
            else args.group_id
        ),
    )

    postgres_config = PostgresConfig.from_environment()
    configure_logging(args.log_level)

    try:
        inserted, duplicates = consume_to_postgres(
            kafka_config=kafka_config,
            postgres_config=postgres_config,
            max_messages=args.max_messages,
            fail_after_db_write=args.fail_after_db_write,
        )
    except Exception as exc:
        print(f"Kafka/PostgreSQL ingestion error: {exc}")
        return 1

    print(
        f"Ingestion complete: inserted={inserted} duplicates={duplicates}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
