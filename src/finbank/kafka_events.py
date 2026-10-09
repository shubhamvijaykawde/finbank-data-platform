"""Event serialization helpers for FinBank Kafka messages."""

from __future__ import annotations

from datetime import datetime
import json
from typing import Any


EVENT_TYPE = "transaction.created"
EVENT_SCHEMA_VERSION = 1
REQUIRED_TRANSACTION_FIELDS = {
    "transaction_id",
    "customer_id",
    "account_id",
    "merchant_id",
    "transaction_type",
    "amount",
    "currency",
    "timestamp",
    "country",
    "city",
    "payment_method",
    "status",
}
REQUIRED_EVENT_FIELDS = {
    "event_id",
    "event_type",
    "schema_version",
    "event_timestamp",
    "transaction",
}


def build_transaction_event(
    transaction: dict[str, str],
    event_id: str | None = None,
) -> dict[str, Any]:
    """Build a versioned JSON event from one generated transaction row."""
    missing = REQUIRED_TRANSACTION_FIELDS - transaction.keys()
    if missing:
        missing_names = ", ".join(sorted(missing))
        raise ValueError(
            f"Transaction row is missing required fields: {missing_names}"
        )

    transaction_id = transaction["transaction_id"]
    resolved_event_id = event_id or transaction_id
    if not resolved_event_id.strip():
        raise ValueError("event_id cannot be empty")

    event_timestamp = transaction["timestamp"]

    # Confirm the timestamp is ISO-8601 before publishing it as an event field.
    datetime.fromisoformat(event_timestamp.replace("Z", "+00:00"))

    return {
        "event_id": resolved_event_id,
        "event_type": EVENT_TYPE,
        "schema_version": EVENT_SCHEMA_VERSION,
        "event_timestamp": event_timestamp,
        "transaction": transaction,
    }


def serialize_event(event: dict[str, Any]) -> bytes:
    """Serialize one event to compact, deterministic UTF-8 JSON."""
    return json.dumps(
        event,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def deserialize_event(payload: bytes | str) -> dict[str, Any]:
    """Deserialize one UTF-8 JSON payload and validate its event envelope."""
    if isinstance(payload, bytes):
        payload = payload.decode("utf-8")

    value = json.loads(payload)
    if not isinstance(value, dict):
        raise ValueError("Kafka payload must be a JSON object")

    missing = REQUIRED_EVENT_FIELDS - value.keys()
    if missing:
        missing_names = ", ".join(sorted(missing))
        raise ValueError(
            f"Event is missing required fields: {missing_names}"
        )

    if value["event_type"] != EVENT_TYPE:
        raise ValueError(
            f"Unsupported event_type: {value['event_type']}"
        )

    if value["schema_version"] != EVENT_SCHEMA_VERSION:
        raise ValueError(
            f"Unsupported schema_version: {value['schema_version']}"
        )

    transaction = value["transaction"]
    if not isinstance(transaction, dict):
        raise ValueError("Event transaction must be a JSON object")

    missing_transaction_fields = REQUIRED_TRANSACTION_FIELDS - transaction.keys()
    if missing_transaction_fields:
        missing_names = ", ".join(sorted(missing_transaction_fields))
        raise ValueError(
            "Event transaction is missing required fields: "
            f"{missing_names}"
        )

    if not isinstance(value["event_id"], str) or not value["event_id"].strip():
        raise ValueError("event_id cannot be empty")

    return value
