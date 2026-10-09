from datetime import datetime, timezone

import pytest

from finbank.data_quality import (
    DUPLICATE_TRANSACTION,
    FUTURE_TIMESTAMP,
    INVALID_CURRENCY,
    INVALID_STATUS,
    MISSING_ACCOUNT,
    MISSING_CUSTOMER,
    NEGATIVE_AMOUNT,
    ValidationResult,
    validate_transaction_fields,
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


def event(transaction=None):
    return {
        "event_id": "EVENT0001",
        "event_type": "transaction.created",
        "schema_version": 1,
        "event_timestamp": "2025-12-30T12:30:00+00:00",
        "transaction": dict(TRANSACTION if transaction is None else transaction),
    }


def codes(result: ValidationResult) -> set[str]:
    return {code for code, _ in result.reasons}


def test_valid_transaction_has_no_rejections() -> None:
    result = validate_transaction_fields(event())

    assert result.valid
    assert not result.reasons


def test_missing_customer_and_account_are_rejected() -> None:
    transaction = dict(TRANSACTION, customer_id="", account_id="")

    result = validate_transaction_fields(event(transaction))

    assert not result.valid
    assert {MISSING_CUSTOMER, MISSING_ACCOUNT} <= codes(result)


def test_non_positive_amount_is_rejected() -> None:
    transaction = dict(TRANSACTION, amount="-1.00")

    result = validate_transaction_fields(event(transaction))

    assert not result.valid
    assert NEGATIVE_AMOUNT in codes(result)


def test_invalid_currency_is_rejected() -> None:
    transaction = dict(TRANSACTION, currency="XXX")

    result = validate_transaction_fields(event(transaction))

    assert INVALID_CURRENCY in codes(result)


def test_invalid_status_is_rejected() -> None:
    transaction = dict(TRANSACTION, status="reversed")

    result = validate_transaction_fields(event(transaction))

    assert INVALID_STATUS in codes(result)


def test_future_timestamp_is_rejected() -> None:
    transaction = dict(TRANSACTION, timestamp="2099-01-01T00:00:00+00:00")
    now = datetime(2026, 10, 8, tzinfo=timezone.utc)

    result = validate_transaction_fields(event(transaction), now=now)

    assert FUTURE_TIMESTAMP in codes(result)


def test_malformed_transaction_object_is_rejected() -> None:
    value = event()
    value["transaction"] = None

    result = validate_transaction_fields(value)

    assert not result.valid
    assert result.reasons[0][0] == "MALFORMED_EVENT"


def test_duplicate_code_is_reserved_for_database_backed_validation() -> None:
    assert DUPLICATE_TRANSACTION == "DUPLICATE_TRANSACTION_ID"


def test_zero_amount_is_rejected() -> None:
    transaction = dict(TRANSACTION, amount="0.00")

    result = validate_transaction_fields(event(transaction))

    assert NEGATIVE_AMOUNT in codes(result)


def test_multiple_quality_failures_are_returned_together() -> None:
    transaction = dict(
        TRANSACTION,
        customer_id="",
        account_id="",
        amount="-2.00",
        currency="XXX",
        status="reversed",
    )

    result = validate_transaction_fields(event(transaction))

    expected = {
        MISSING_CUSTOMER,
        MISSING_ACCOUNT,
        NEGATIVE_AMOUNT,
        INVALID_CURRENCY,
        INVALID_STATUS,
    }

    assert expected <= codes(result)
