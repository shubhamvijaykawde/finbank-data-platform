from __future__ import annotations

from dataclasses import fields
from datetime import timezone
from decimal import Decimal

from finbank.config import GeneratorConfig
from finbank.generator import (
    CURRENCY_BY_COUNTRY,
    REFERENCE_DATETIME,
    STATUSES,
    generate_dataset,
)


def make_config(**overrides: object) -> GeneratorConfig:
    values = {
        "customers": 100,
        "accounts": 120,
        "merchants": 20,
        "transactions": 1_000,
        "suspicious_customer_pct": 5.0,
        "seed": 42,
    }
    values.update(overrides)
    return GeneratorConfig(**values)


def test_expected_sizes() -> None:
    dataset = generate_dataset(make_config())

    assert len(dataset.customers) == 100
    assert len(dataset.accounts) == 120
    assert len(dataset.merchants) == 20
    assert len(dataset.transactions) == 1_000


def test_ids_are_unique() -> None:
    dataset = generate_dataset(make_config())

    assert len({row.customer_id for row in dataset.customers}) == len(dataset.customers)
    assert len({row.account_id for row in dataset.accounts}) == len(dataset.accounts)
    assert len({row.merchant_id for row in dataset.merchants}) == len(dataset.merchants)
    assert len({row.transaction_id for row in dataset.transactions}) == len(dataset.transactions)


def test_required_fields_exist() -> None:
    expected = {
        "Customer": {
            "customer_id", "first_name", "last_name", "date_of_birth", "country",
            "city", "registration_date", "customer_segment",
        },
        "Account": {
            "account_id", "customer_id", "account_type", "currency",
            "account_open_date", "initial_balance",
        },
        "Merchant": {
            "merchant_id", "merchant_name", "merchant_category", "country", "city",
        },
        "Transaction": {
            "transaction_id", "customer_id", "account_id", "merchant_id",
            "transaction_type", "amount", "currency", "timestamp", "country",
            "city", "payment_method", "status",
        },
    }

    from finbank.models import Account, Customer, Merchant, Transaction

    for model in (Customer, Account, Merchant, Transaction):
        assert {field.name for field in fields(model)} == expected[model.__name__]


def test_account_references_point_to_valid_customers() -> None:
    dataset = generate_dataset(make_config())
    customer_ids = {customer.customer_id for customer in dataset.customers}

    assert all(account.customer_id in customer_ids for account in dataset.accounts)


def test_transaction_references_point_to_valid_related_entities() -> None:
    dataset = generate_dataset(make_config())
    customers = {customer.customer_id: customer for customer in dataset.customers}
    accounts = {account.account_id: account for account in dataset.accounts}
    merchants = {merchant.merchant_id for merchant in dataset.merchants}

    for transaction in dataset.transactions:
        assert transaction.customer_id in customers
        assert transaction.account_id in accounts
        assert accounts[transaction.account_id].customer_id == transaction.customer_id
        assert transaction.merchant_id in merchants


def test_amounts_currencies_timestamps_and_status_are_valid() -> None:
    dataset = generate_dataset(make_config())
    valid_currencies = set(CURRENCY_BY_COUNTRY.values()) | {"EUR", "GBP"}

    for transaction in dataset.transactions:
        assert transaction.amount > Decimal("0")
        assert transaction.currency in valid_currencies
        assert transaction.timestamp.tzinfo is not None
        assert transaction.timestamp <= REFERENCE_DATETIME
        assert transaction.status in STATUSES


def test_deterministic_seed_reproduces_full_dataset() -> None:
    first = generate_dataset(make_config(seed=123))
    second = generate_dataset(make_config(seed=123))

    assert first == second


def test_changing_seed_changes_generated_data() -> None:
    first = generate_dataset(make_config(seed=123))
    second = generate_dataset(make_config(seed=124))

    assert first != second


def test_suspicious_customer_count_follows_configuration() -> None:
    dataset = generate_dataset(make_config(customers=200, accounts=220, suspicious_customer_pct=7.5))

    assert len(dataset.suspicious_customer_ids) == 15
    assert dataset.suspicious_customer_ids <= {
        customer.customer_id for customer in dataset.customers
    }


def test_suspicious_customers_are_represented_in_transactions() -> None:
    dataset = generate_dataset(make_config(customers=100, transactions=500, suspicious_customer_pct=5))
    transaction_customer_ids = {transaction.customer_id for transaction in dataset.transactions}

    assert dataset.suspicious_customer_ids <= transaction_customer_ids


def test_no_future_timestamps_are_generated() -> None:
    dataset = generate_dataset(make_config())
    assert max(transaction.timestamp for transaction in dataset.transactions) <= REFERENCE_DATETIME


def test_reference_datetime_is_timezone_aware() -> None:
    assert REFERENCE_DATETIME.tzinfo == timezone.utc
