"""Create the FinBank Kafka transaction topic."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPOSITORY_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

from finbank.kafka_config import KafkaConfig
from finbank.logging_config import configure_logging


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Create the FinBank Kafka transactions topic."
    )
    parser.add_argument("--bootstrap-servers", default=None)
    parser.add_argument("--topic", default=None)
    parser.add_argument("--partitions", type=int, default=1)
    parser.add_argument("--replication-factor", type=int, default=1)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

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
    config.validate()

    if args.partitions < 1:
        raise SystemExit("--partitions must be at least 1")
    if args.replication_factor < 1:
        raise SystemExit("--replication-factor must be at least 1")

    try:
        from kafka import KafkaAdminClient
        from kafka.admin import NewTopic
        from kafka.errors import TopicAlreadyExistsError
    except ImportError as exc:
        raise SystemExit(
            "kafka-python is not installed. Run: pip install -r requirements.txt"
        ) from exc

    configure_logging(args.log_level)

    admin = KafkaAdminClient(
        bootstrap_servers=config.bootstrap_servers,
        client_id="finbank-topic-admin",
    )

    try:
        topic = NewTopic(
            name=config.topic,
            num_partitions=args.partitions,
            replication_factor=args.replication_factor,
        )

        try:
            admin.create_topics(new_topics=[topic], validate_only=False)
        except TopicAlreadyExistsError:
            print(
                f"Topic already exists: {config.topic}"
            )
            return 0

        print(
            f"Created topic '{config.topic}' with "
            f"{args.partitions} partition(s) and "
            f"replication factor {args.replication_factor}."
        )
        return 0

    finally:
        admin.close()


if __name__ == "__main__":
    raise SystemExit(main())
