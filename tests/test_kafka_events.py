from datetime import datetime, timezone

import pytest

from finbank.kafka_config import KafkaConfig
from finbank.kafka_events import (
    EVENT_SCHEMA_VERSION,
    EVENT_TYPE,
    build_transaction_event,
    deserialize_event,
    serialize_event,
)


TRANSACTION = {
    "transaction_id": "TXN00000001",
    "customer_id": "CUS000001",
    "account_id": "ACC000001",
    "merchant_id": "MER000001",
    "transaction_type": "purchase",
    "amount": "123.45",
    "currency": "EUR",
    "timestamp": "2025-12-30T12:30:00+00:00",
    "country": "Germany",
    "city": "Berlin",
    "payment_method": "card",
    "status": "completed",
}


def test_build_transaction_event_has_stable_identity() -> None:
    event = build_transaction_event(TRANSACTION)

    assert event["event_id"] == TRANSACTION["transaction_id"]
    assert event["event_type"] == EVENT_TYPE
    assert event["schema_version"] == EVENT_SCHEMA_VERSION
    assert event["transaction"] == TRANSACTION


def test_serialization_round_trip() -> None:
    event = build_transaction_event(TRANSACTION)

    payload = serialize_event(event)
    restored = deserialize_event(payload)

    assert restored == event


def test_unsupported_event_type_is_rejected() -> None:
    event = build_transaction_event(TRANSACTION)
    event["event_type"] = "customer.created"

    with pytest.raises(ValueError, match="Unsupported event_type"):
        deserialize_event(serialize_event(event))


def test_event_id_can_differ_from_transaction_id() -> None:
    event = build_transaction_event(TRANSACTION, event_id="EVENT00000099")

    restored = deserialize_event(serialize_event(event))

    assert restored["event_id"] == "EVENT00000099"
    assert restored["transaction"]["transaction_id"] == "TXN00000001"


def test_missing_transaction_field_is_rejected() -> None:
    transaction = dict(TRANSACTION)
    del transaction["merchant_id"]

    with pytest.raises(ValueError, match="missing required fields"):
        build_transaction_event(transaction)


def test_invalid_timestamp_is_rejected() -> None:
    transaction = dict(TRANSACTION)
    transaction["timestamp"] = "not-a-timestamp"

    with pytest.raises(ValueError):
        build_transaction_event(transaction)


def test_kafka_config_validation() -> None:
    config = KafkaConfig()
    config.validate()

    with pytest.raises(ValueError, match="topic cannot be empty"):
        KafkaConfig(topic=" ").validate()


def test_reference_timestamp_is_timezone_aware() -> None:
    timestamp = datetime.fromisoformat(
        TRANSACTION["timestamp"]
    )
    assert timestamp.tzinfo == timezone.utc

