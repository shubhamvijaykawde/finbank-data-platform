"""Kafka-to-PostgreSQL ingestion with Phase 4 data-quality quarantine."""

from __future__ import annotations

import logging
import time
from typing import Callable

from finbank.data_quality import MALFORMED_EVENT, validate_transaction_fields
from finbank.kafka_config import KafkaConfig
from finbank.kafka_events import deserialize_event
from finbank.postgres import RawTransactionWriter
from finbank.postgres_config import PostgresConfig
from finbank.quality_store import ReferenceDataValidator, RejectedRecordWriter
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


def create_quality_consumer(
    kafka_config: KafkaConfig,
    auto_offset_reset: str = "latest",
):
    KafkaConsumer = _require_kafka_consumer()
    return KafkaConsumer(
        kafka_config.topic,
        bootstrap_servers=kafka_config.bootstrap_servers,
        group_id=kafka_config.consumer_group,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=False,
        key_deserializer=lambda value: value.decode("utf-8"),
        value_deserializer=lambda value: value,
    )


def consume_with_quality(
    kafka_config: KafkaConfig,
    postgres_config: PostgresConfig,
    max_messages: int | None = None,
    consumer_factory: Callable[[KafkaConfig], object] = create_quality_consumer,
    auto_offset_reset: str = "latest",
) -> tuple[int, int, int]:
    """Consume events, quarantine invalid data, ingest valid data.

    Returns (inserted, duplicates, rejected). Every successfully handled
    record—valid or rejected—has its Kafka offset committed only after the
    corresponding PostgreSQL transaction succeeds.
    """
    if auto_offset_reset not in {"earliest", "latest"}:
        raise ValueError("auto_offset_reset must be earliest or latest")

    # The default is latest so a fresh Phase 4 demo consumes only newly
    # published invalid events rather than replaying older Phase 2/3 data.
    consumer = (
        consumer_factory(kafka_config)
        if consumer_factory is not create_quality_consumer
        else create_quality_consumer(kafka_config, auto_offset_reset)
    )
    validator = ReferenceDataValidator(postgres_config)
    rejected_writer = RejectedRecordWriter(postgres_config)
    raw_writer = RawTransactionWriter(postgres_config)

    inserted = 0
    duplicates = 0
    rejected = 0
    processed = 0
    started = time.monotonic()

    try:
        for message in consumer:
            raw_payload = (
                message.value.decode("utf-8", errors="replace")
                if isinstance(message.value, bytes)
                else str(message.value)
            )

            try:
                event = deserialize_event(message.value)
            except (UnicodeDecodeError, ValueError, TypeError) as exc:
                rejected_writer.quarantine(
                    event_id=None,
                    transaction_id=None,
                    rejection_code=MALFORMED_EVENT,
                    rejection_reason=str(exc),
                    raw_payload=raw_payload,
                    kafka_topic=message.topic,
                    kafka_partition=message.partition,
                    kafka_offset=message.offset,
                )
                rejected += 1
                consumer.commit()
                processed += 1
                LOGGER.warning(
                    "Quarantined malformed event topic=%s partition=%s offset=%s reason=%s",
                    message.topic,
                    message.partition,
                    message.offset,
                    exc,
                )
            else:
                result = validate_transaction_fields(event)
                reasons = list(result.reasons)
                if not result.valid:
                    if not reasons:
                        raise RuntimeError("validation failed without a rejection reason")
                    codes = [code for code, _ in reasons]
                    reasons_text = "; ".join(reason for _, reason in reasons)
                    rejected_writer.quarantine(
                        event_id=event.get("event_id"),
                        transaction_id=event["transaction"].get("transaction_id"),
                        rejection_code="MULTIPLE" if len(codes) > 1 else codes[0],
                        rejection_reason=reasons_text,
                        raw_payload=raw_payload,
                        kafka_topic=message.topic,
                        kafka_partition=message.partition,
                        kafka_offset=message.offset,
                    )
                    rejected += 1
                    consumer.commit()
                    processed += 1
                    LOGGER.warning(
                        "Quarantined event_id=%s offset=%s reasons=%s",
                        event["event_id"],
                        message.offset,
                        "; ".join(codes),
                    )
                else:
                    db_reasons = validator.validate(event)
                    if db_reasons:
                        codes = [code for code, _ in db_reasons]
                        reasons_text = "; ".join(reason for _, reason in db_reasons)
                        rejected_writer.quarantine(
                            event_id=event.get("event_id"),
                            transaction_id=event["transaction"].get("transaction_id"),
                            rejection_code="MULTIPLE" if len(codes) > 1 else codes[0],
                            rejection_reason=reasons_text,
                            raw_payload=raw_payload,
                            kafka_topic=message.topic,
                            kafka_partition=message.partition,
                            kafka_offset=message.offset,
                        )
                        rejected += 1
                        consumer.commit()
                        processed += 1
                        LOGGER.warning(
                            "Quarantined event_id=%s offset=%s reasons=%s",
                            event["event_id"],
                            message.offset,
                            "; ".join(codes),
                        )
                    else:
                        was_inserted = raw_writer.insert_event(
                            event=event,
                            kafka_topic=message.topic,
                            kafka_partition=message.partition,
                            kafka_offset=message.offset,
                        )
                        if was_inserted:
                            inserted += 1
                        else:
                            duplicates += 1
                            LOGGER.info(
                                "Duplicate replay ignored: event_id=%s transaction_id=%s offset=%s",
                                event["event_id"],
                                event["transaction"]["transaction_id"],
                                message.offset,
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
            operation="phase4_quality_consume",
        )
        raise
    finally:
        raw_writer.close()
        rejected_writer.close()
        validator.close()
        consumer.close(autocommit=False)

    record_operational_event(
        "ingestion_run",
        "consumer",
        mode="phase4",
        processed=processed,
        inserted=inserted,
        duplicates=duplicates,
        rejected=rejected,
        elapsed_seconds=round(time.monotonic() - started, 6),
    )

    LOGGER.info(
        "Phase 4 ingestion complete: processed=%s inserted=%s duplicates=%s rejected=%s",
        processed,
        inserted,
        duplicates,
        rejected,
    )
    return inserted, duplicates, rejected
