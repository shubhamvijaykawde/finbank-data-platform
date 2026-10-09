"""Kafka producer for FinBank transaction events."""

from __future__ import annotations

import csv
import logging
from pathlib import Path
from typing import Callable

from finbank.kafka_config import KafkaConfig
from finbank.kafka_events import build_transaction_event, serialize_event

LOGGER = logging.getLogger(__name__)


def _require_kafka_producer() -> tuple[type, type, type]:
    try:
        from kafka import KafkaProducer
        from kafka.serializer import DefaultSerializer, Serializer
    except ImportError as exc:
        raise RuntimeError(
            "kafka-python is not installed. Run: pip install -r requirements.txt"
        ) from exc

    return KafkaProducer, DefaultSerializer, Serializer


def create_producer(config: KafkaConfig):
    """Create a producer using kafka-python 3.x Serializer interfaces."""
    KafkaProducer, DefaultSerializer, Serializer = _require_kafka_producer()

    class FinBankEventSerializer(Serializer):
        """Serialize FinBank event dictionaries to deterministic JSON bytes."""

        def serialize(self, topic, headers, data):
            if data is None:
                return None
            return serialize_event(data)

    return KafkaProducer(
        bootstrap_servers=config.bootstrap_servers,
        key_serializer=DefaultSerializer(),
        value_serializer=FinBankEventSerializer(),
        acks="all",
        retries=3,
        linger_ms=5,
        batch_size=16_384,
        max_block_ms=10_000,
        request_timeout_ms=10_000,
    )


def publish_transactions(
    input_path: Path,
    config: KafkaConfig,
    max_records: int | None = None,
    producer_factory: Callable[[KafkaConfig], object] = create_producer,
) -> int:
    """Publish generated transaction CSV rows to the Kafka topic."""
    if not input_path.exists():
        raise FileNotFoundError(
            f"Transaction CSV not found: {input_path}"
        )

    config.validate()

    producer = producer_factory(config)
    published = 0

    try:
        with input_path.open(
            "r",
            newline="",
            encoding="utf-8",
        ) as handle:
            reader = csv.DictReader(handle)

            for row in reader:
                if max_records is not None and published >= max_records:
                    break

                event = build_transaction_event(row)
                transaction_id = row["transaction_id"]

                future = producer.send(
                    config.topic,
                    key=transaction_id,
                    value=event,
                )
                future.get(timeout=15)
                published += 1

                if published % 1_000 == 0:
                    LOGGER.info(
                        "Published %s transaction events",
                        published,
                    )

        producer.flush()

    finally:
        producer.close()

    LOGGER.info(
        "Kafka publish complete: published=%s topic=%s",
        published,
        config.topic,
    )
    return published
