"""Phase 4 transaction validation and quarantine reason codes."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from finbank.generator import REFERENCE_DATETIME, STATUSES
from finbank.kafka_events import REQUIRED_TRANSACTION_FIELDS

VALID_CURRENCIES = {"EUR", "GBP"}

MISSING_CUSTOMER = "MISSING_CUSTOMER_ID"
MISSING_ACCOUNT = "MISSING_ACCOUNT_ID"
NEGATIVE_AMOUNT = "NON_POSITIVE_AMOUNT"
INVALID_CURRENCY = "INVALID_CURRENCY"
UNKNOWN_MERCHANT = "UNKNOWN_MERCHANT"
DUPLICATE_TRANSACTION = "DUPLICATE_TRANSACTION_ID"
FUTURE_TIMESTAMP = "FUTURE_TIMESTAMP"
INVALID_STATUS = "INVALID_STATUS"
MISMATCHED_ACCOUNT_CUSTOMER = "ACCOUNT_CUSTOMER_MISMATCH"
MALFORMED_EVENT = "MALFORMED_EVENT"


@dataclass(frozen=True)
class ValidationResult:
    valid: bool
    reasons: tuple[tuple[str, str], ...]


def validate_transaction_fields(
    event: dict[str, Any],
    *,
    now: datetime | None = None,
) -> ValidationResult:
    """Validate transaction-level business/data-quality rules."""
    reasons: list[tuple[str, str]] = []
    transaction = event.get("transaction")

    if not isinstance(transaction, dict):
        return ValidationResult(
            valid=False,
            reasons=((MALFORMED_EVENT, "transaction must be a JSON object"),),
        )

    for field in REQUIRED_TRANSACTION_FIELDS:
        value = transaction.get(field)
        if value is None or (isinstance(value, str) and not value.strip()):
            code = {
                "customer_id": MISSING_CUSTOMER,
                "account_id": MISSING_ACCOUNT,
            }.get(field)
            if code is not None:
                reasons.append((code, f"{field} is missing or empty"))

    amount_raw = transaction.get("amount")
    try:
        amount = Decimal(str(amount_raw))
        if amount <= 0:
            reasons.append((NEGATIVE_AMOUNT, "amount must be greater than zero"))
    except (InvalidOperation, ValueError, TypeError):
        reasons.append((NEGATIVE_AMOUNT, "amount must be a valid decimal number"))

    if transaction.get("currency") not in VALID_CURRENCIES:
        reasons.append((
            INVALID_CURRENCY,
            f"unsupported currency: {transaction.get('currency')!r}",
        ))

    timestamp_raw = transaction.get("timestamp")
    try:
        timestamp = datetime.fromisoformat(
            str(timestamp_raw).replace("Z", "+00:00")
        )
        if timestamp.tzinfo is None:
            reasons.append((FUTURE_TIMESTAMP, "timestamp must include timezone information"))
        else:
            compare_now = now or datetime.now(timezone.utc)
            if timestamp > compare_now:
                reasons.append((FUTURE_TIMESTAMP, "transaction timestamp is in the future"))
    except (ValueError, TypeError):
        reasons.append((FUTURE_TIMESTAMP, "timestamp is not valid ISO-8601 datetime"))

    if transaction.get("status") not in STATUSES:
        reasons.append((
            INVALID_STATUS,
            f"unsupported status: {transaction.get('status')!r}",
        ))

    return ValidationResult(
        valid=not reasons,
        reasons=tuple(reasons),
    )


def validate_generated_reference_timestamp(timestamp: datetime) -> None:
    """Validate against the Phase 1 fixed reference date in unit tests."""
    if timestamp > REFERENCE_DATETIME:
        raise ValueError("generated timestamp exceeds Phase 1 reference time")
