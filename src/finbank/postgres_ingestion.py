"""Kafka-to-PostgreSQL ingestion for FinBank Phase 3."""

from __future__ import annotations

import logging
import time
from typing import Any, Callable

from finbank.kafka_config import KafkaConfig
from finbank.kafka_events import deserialize_event
from finbank.postgres import RawTransactionWriter
from finbank.postgres_config import PostgresConfig
from finbank.operational_events import record_operational_event

LOGGER = logging.getLogger(__name__)


def _require_kafka_consumer() -> type:
    try:
        from kafka import KafkaConsumer
    except ImportError as exc:
        raise RuntimeError(
            "kafka-python is not installed. Run: pip install -r requirements.txt"
        ) from exc
    return KafkaConsumer


def create_db_consumer(kafka_config: KafkaConfig):
    KafkaConsumer = _require_kafka_consumer()

    return KafkaConsumer(
        kafka_config.topic,
        bootstrap_servers=kafka_config.bootstrap_servers,
        group_id=kafka_config.consumer_group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        key_deserializer=lambda value: value.decode("utf-8"),
        value_deserializer=lambda value: value,
    )


def consume_to_postgres(
    kafka_config: KafkaConfig,
    postgres_config: PostgresConfig,
    max_messages: int | None = None,
    consumer_factory: Callable[[KafkaConfig], object] = create_db_consumer,
    fail_after_db_write: bool = False,
) -> tuple[int, int]:
    """Consume Kafka events, persist them, then commit the Kafka offset.

    Returns (inserted_count, duplicate_count).
    """
    consumer = consumer_factory(kafka_config)
    writer = RawTransactionWriter(postgres_config)
    inserted_count = 0
    duplicate_count = 0
    messages_processed = 0
    failure_triggered = False
    started = time.monotonic()

    try:
        for message in consumer:
            event = deserialize_event(message.value)

            inserted = writer.insert_event(
                event=event,
                kafka_topic=message.topic,
                kafka_partition=message.partition,
                kafka_offset=message.offset,
            )

            if inserted:
                inserted_count += 1
                LOGGER.info(
                    "Inserted transaction_id=%s topic=%s partition=%s offset=%s",
                    event["transaction"]["transaction_id"],
                    message.topic,
                    message.partition,
                    message.offset,
                )
            else:
                duplicate_count += 1
                LOGGER.warning(
                    "Duplicate event ignored: event_id=%s transaction_id=%s "
                    "topic=%s partition=%s offset=%s",
                    event["event_id"],
                    event["transaction"]["transaction_id"],
                    message.topic,
                    message.partition,
                    message.offset,
                )

            # This controlled failure demonstrates the important crash window:
            # database commit succeeds, Kafka offset commit does not.
            if fail_after_db_write and not failure_triggered:
                failure_triggered = True
                raise RuntimeError(
                    "Simulated failure after PostgreSQL commit and before Kafka offset commit"
                )

            consumer.commit()
            messages_processed += 1

            if (
                max_messages is not None
                and messages_processed >= max_messages
            ):
                break

    except Exception as exc:
        record_operational_event(
            "error",
            "consumer",
            message=str(exc),
            error_class=type(exc).__name__,
            operation="phase3_consume_to_postgres",
        )
        raise
    finally:
        writer.close()
        # Defensive: never let close() auto-commit an unprocessed offset.
        consumer.close(autocommit=False)

    record_operational_event(
        "ingestion_run",
        "consumer",
        mode="phase3",
        processed=messages_processed,
        inserted=inserted_count,
        duplicates=duplicate_count,
        rejected=0,
        elapsed_seconds=round(time.monotonic() - started, 6),
    )

    LOGGER.info(
        "Kafka-to-PostgreSQL ingestion complete: processed=%s inserted=%s duplicates=%s "
        "topic=%s group=%s",
        messages_processed,
        inserted_count,
        duplicate_count,
        kafka_config.topic,
        kafka_config.consumer_group,
    )

    return inserted_count, duplicate_count
