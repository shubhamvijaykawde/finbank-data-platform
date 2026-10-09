"""Core Phase 1 data models for FinBank."""

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal


@dataclass(frozen=True)
class Customer:
    customer_id: str
    first_name: str
    last_name: str
    date_of_birth: date
    country: str
    city: str
    registration_date: date
    customer_segment: str


@dataclass(frozen=True)
class Account:
    account_id: str
    customer_id: str
    account_type: str
    currency: str
    account_open_date: date
    initial_balance: Decimal


@dataclass(frozen=True)
class Merchant:
    merchant_id: str
    merchant_name: str
    merchant_category: str
    country: str
    city: str


@dataclass(frozen=True)
class Transaction:
    transaction_id: str
    customer_id: str
    account_id: str
    merchant_id: str
    transaction_type: str
    amount: Decimal
    currency: str
    timestamp: datetime
    country: str
    city: str
    payment_method: str
    status: str


@dataclass(frozen=True)
class GeneratedDataset:
    customers: list[Customer]
    accounts: list[Account]
    merchants: list[Merchant]
    transactions: list[Transaction]
    suspicious_customer_ids: frozenset[str]
