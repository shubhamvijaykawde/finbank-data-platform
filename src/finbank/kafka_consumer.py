"""Kafka consumer for FinBank transaction events."""

from __future__ import annotations

import logging
from typing import Any, Callable

from finbank.kafka_config import KafkaConfig
from finbank.kafka_events import deserialize_event
from finbank.operational_events import record_operational_event
import time

LOGGER = logging.getLogger(__name__)


def _require_kafka_consumer() -> type:
    try:
        from kafka import KafkaConsumer
    except ImportError as exc:
        raise RuntimeError(
            "kafka-python is not installed. Run: pip install -r requirements.txt"
        ) from exc

    return KafkaConsumer


def create_consumer(config: KafkaConfig):
    """Create a consumer with explicit, manual offset commits."""
    KafkaConsumer = _require_kafka_consumer()

    return KafkaConsumer(
        config.topic,
        bootstrap_servers=config.bootstrap_servers,
        group_id=config.consumer_group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        key_deserializer=lambda value: value.decode("utf-8"),
        value_deserializer=lambda value: value,
    )


def consume_transactions(
    config: KafkaConfig,
    max_messages: int | None = None,
    consumer_factory: Callable[[KafkaConfig], object] = create_consumer,
) -> int:
    """Consume and validate transaction events from Kafka.

    Phase 2 deliberately has no database side effect. The message is considered
    processed only after JSON/event validation succeeds, then its offset is
    committed. This makes redelivery behavior observable before PostgreSQL is
    introduced in Phase 3.
    """
    config.validate()

    consumer = consumer_factory(config)
    processed = 0
    started = time.monotonic()

    try:
        for message in consumer:
            try:
                event = deserialize_event(message.value)
            except (UnicodeDecodeError, ValueError, TypeError) as exc:
                record_operational_event(
                    "error",
                    "consumer",
                    message=str(exc),
                    error_class=type(exc).__name__,
                    operation="phase2_event_validation",
                )
                LOGGER.error(
                    "Event validation failed at %s[%s]@%s: %s",
                    message.topic,
                    message.partition,
                    message.offset,
                    exc,
                )
                raise

            transaction: dict[str, Any] = event["transaction"]

            LOGGER.info(
                "Consumed event_id=%s transaction_id=%s topic=%s "
                "partition=%s offset=%s key=%s",
                event["event_id"],
                transaction["transaction_id"],
                message.topic,
                message.partition,
                message.offset,
                message.key,
            )

            consumer.commit()
            processed += 1

            if max_messages is not None and processed >= max_messages:
                break

    except Exception as exc:
        record_operational_event(
            "error",
            "consumer",
            message=str(exc),
            error_class=type(exc).__name__,
            operation="phase2_consume",
        )
        raise
    finally:
        # Defensive: never let close() auto-commit an unprocessed offset.
        consumer.close(autocommit=False)

    record_operational_event(
        "ingestion_run",
        "consumer",
        mode="phase2",
        processed=processed,
        inserted=processed,
        duplicates=0,
        rejected=0,
        elapsed_seconds=round(time.monotonic() - started, 6),
    )

    LOGGER.info(
        "Kafka consume complete: processed=%s topic=%s group=%s",
        processed,
        config.topic,
        config.consumer_group,
    )
    return processed
