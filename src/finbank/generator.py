"""Deterministic, related banking data generator for FinBank Phase 1."""

from __future__ import annotations

import csv
import logging
import random
from dataclasses import fields
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

from finbank.config import GeneratorConfig
from finbank.models import Account, Customer, GeneratedDataset, Merchant, Transaction

LOGGER = logging.getLogger(__name__)

REFERENCE_DATETIME = datetime(2025, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
REFERENCE_DATE = REFERENCE_DATETIME.date()

LOCATIONS: dict[str, list[str]] = {
    "Germany": ["Berlin", "Hamburg", "Munich", "Frankfurt", "Cologne", "Stuttgart"],
    "France": ["Paris", "Lyon", "Marseille", "Toulouse", "Nice"],
    "United Kingdom": ["London", "Manchester", "Birmingham", "Liverpool"],
    "Netherlands": ["Amsterdam", "Rotterdam", "Utrecht"],
    "Spain": ["Madrid", "Barcelona", "Valencia", "Seville"],
}

CURRENCY_BY_COUNTRY = {
    "Germany": "EUR",
    "France": "EUR",
    "United Kingdom": "GBP",
    "Netherlands": "EUR",
    "Spain": "EUR",
}

FIRST_NAMES = [
    "Anna", "Daniel", "Emma", "Felix", "Hannah", "Jonas", "Laura", "Liam",
    "Mia", "Noah", "Olivia", "Paul", "Sofia", "Thomas", "Julia", "Max",
    "Lea", "Nina", "Elias", "Marie",
]

LAST_NAMES = [
    "Bauer", "Bennett", "Dubois", "Evans", "Fischer", "Garcia", "Hoffmann",
    "Jansen", "Klein", "Lambert", "Martin", "Meyer", "Muller", "Parker",
    "Rossi", "Schmidt", "Taylor", "Thomas", "Wagner", "Wilson",
]

MERCHANT_PREFIXES = [
    "North", "Green", "Central", "Urban", "Prime", "Blue", "Metro", "Golden",
    "River", "Oak", "Silver", "Summit",
]

MERCHANT_TYPES = {
    "grocery": ["Market", "Grocers", "Foods"],
    "electronics": ["Electronics", "Digital", "Tech"],
    "travel": ["Travel", "Air", "Tours"],
    "restaurants": ["Kitchen", "Bistro", "Dining"],
    "fashion": ["Fashion", "Apparel", "Boutique"],
    "entertainment": ["Cinema", "Media", "Games"],
    "utilities": ["Utilities", "Energy", "Services"],
}

MERCHANT_CATEGORIES = tuple(MERCHANT_TYPES)
ACCOUNT_TYPES = ("checking", "savings")
PAYMENT_METHODS = ("card", "contactless", "online", "mobile_wallet")
TRANSACTION_TYPES = ("purchase", "online_purchase", "subscription", "cash_withdrawal")
STATUSES = ("completed", "pending", "failed")
CUSTOMER_SEGMENTS = ("standard", "premium", "private")


def _money(value: float) -> Decimal:
    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _random_date(rng: random.Random, start: date, end: date) -> date:
    days = (end - start).days
    return start + timedelta(days=rng.randint(0, days))


def _random_timestamp(rng: random.Random, start: datetime, end: datetime) -> datetime:
    seconds = int((end - start).total_seconds())
    return start + timedelta(seconds=rng.randint(0, seconds))


def _pick_location(rng: random.Random, country: str) -> str:
    return rng.choice(LOCATIONS[country])


def _choose_customer_count(total: int, percentage: float) -> int:
    if percentage <= 0:
        return 0
    return min(total, max(1, int(round(total * percentage / 100))))


def generate_customers(rng: random.Random, count: int) -> list[Customer]:
    customers: list[Customer] = []
    for index in range(1, count + 1):
        country = rng.choice(list(LOCATIONS))
        registration_date = _random_date(rng, date(2015, 1, 1), REFERENCE_DATE - timedelta(days=30))
        dob = _random_date(rng, date(1950, 1, 1), registration_date - timedelta(days=18 * 365))
        customers.append(
            Customer(
                customer_id=f"CUS{index:06d}",
                first_name=rng.choice(FIRST_NAMES),
                last_name=rng.choice(LAST_NAMES),
                date_of_birth=dob,
                country=country,
                city=_pick_location(rng, country),
                registration_date=registration_date,
                customer_segment=rng.choices(
                    CUSTOMER_SEGMENTS,
                    weights=(0.72, 0.23, 0.05),
                    k=1,
                )[0],
            )
        )
    return customers


def generate_accounts(
    rng: random.Random,
    customers: list[Customer],
    count: int,
) -> tuple[list[Account], dict[str, list[Account]]]:
    accounts: list[Account] = []
    accounts_by_customer: dict[str, list[Account]] = {customer.customer_id: [] for customer in customers}

    assignments: list[str] = [customer.customer_id for customer in customers]
    extra_assignments = count - len(customers)
    customer_ids = [customer.customer_id for customer in customers]
    assignments.extend(rng.choice(customer_ids) for _ in range(extra_assignments))
    rng.shuffle(assignments)

    customer_lookup = {customer.customer_id: customer for customer in customers}

    for index, customer_id in enumerate(assignments, start=1):
        customer = customer_lookup[customer_id]
        account_open_date = _random_date(
            rng,
            customer.registration_date,
            REFERENCE_DATE - timedelta(days=7),
        )
        home_currency = CURRENCY_BY_COUNTRY[customer.country]
        currency = home_currency if rng.random() < 0.9 else rng.choice(("EUR", "GBP"))
        account = Account(
            account_id=f"ACC{index:06d}",
            customer_id=customer_id,
            account_type=rng.choice(ACCOUNT_TYPES),
            currency=currency,
            account_open_date=account_open_date,
            initial_balance=_money(rng.uniform(100, 15_000)),
        )
        accounts.append(account)
        accounts_by_customer[customer_id].append(account)

    return accounts, accounts_by_customer


def generate_merchants(rng: random.Random, count: int) -> list[Merchant]:
    merchants: list[Merchant] = []
    countries = list(LOCATIONS)
    for index in range(1, count + 1):
        country = rng.choice(countries)
        category = rng.choice(MERCHANT_CATEGORIES)
        name = f"{rng.choice(MERCHANT_PREFIXES)} {rng.choice(MERCHANT_TYPES[category])} {index:03d}"
        merchants.append(
            Merchant(
                merchant_id=f"MER{index:06d}",
                merchant_name=name,
                merchant_category=category,
                country=country,
                city=_pick_location(rng, country),
            )
        )
    return merchants


def _generate_amount(rng: random.Random, profile: str) -> Decimal:
    if profile == "normal":
        amount = rng.lognormvariate(3.6, 0.65)
    elif profile == "high":
        amount = rng.lognormvariate(5.3, 0.55)
    else:
        # Suspicious customers still make ordinary purchases, but occasionally
        # produce much larger, varied values rather than one repeated pattern.
        if rng.random() < 0.7:
            amount = rng.lognormvariate(3.9, 0.7)
        else:
            amount = rng.lognormvariate(8.25, 0.5)
    return max(Decimal("0.01"), _money(amount))


def _transaction_profile(customer: Customer, suspicious_customer_ids: frozenset[str]) -> str:
    if customer.customer_id in suspicious_customer_ids:
        return "suspicious"
    if customer.customer_segment == "private":
        return "high"
    return "normal"


def generate_transactions(
    rng: random.Random,
    customers: list[Customer],
    accounts_by_customer: dict[str, list[Account]],
    merchants: list[Merchant],
    count: int,
    suspicious_customer_ids: frozenset[str],
) -> list[Transaction]:
    if count == 0:
        return []

    customer_lookup = {customer.customer_id: customer for customer in customers}
    merchant_lookup = {merchant.merchant_id: merchant for merchant in merchants}
    eligible_customers = [cid for cid in accounts_by_customer if accounts_by_customer[cid]]

    transactions: list[Transaction] = []
    previous_customer_timestamp: dict[str, datetime] = {}

    required_customers = sorted(cid for cid in suspicious_customer_ids if cid in eligible_customers)
    seeded_customer_ids = required_customers[:count]
    remaining = count - len(seeded_customer_ids)
    transaction_customers = seeded_customer_ids + [rng.choice(eligible_customers) for _ in range(remaining)]

    for index, customer_id in enumerate(transaction_customers, start=1):
        customer = customer_lookup[customer_id]
        account = rng.choice(accounts_by_customer[customer_id])
        merchant = merchant_lookup[rng.choice(list(merchant_lookup))]
        profile = _transaction_profile(customer, suspicious_customer_ids)

        if profile == "suspicious" and rng.random() < 0.35:
            non_home_countries = [c for c in LOCATIONS if c != customer.country]
            transaction_country = rng.choice(non_home_countries)
        else:
            transaction_country = customer.country if rng.random() < 0.92 else merchant.country
        transaction_city = _pick_location(rng, transaction_country)

        previous = previous_customer_timestamp.get(customer_id)
        account_start = datetime.combine(
            account.account_open_date,
            time.min,
            tzinfo=timezone.utc,
        )
        lower_bound = max(
            account_start,
            REFERENCE_DATETIME - timedelta(days=180),
        )
        if profile == "suspicious" and previous is not None and previous >= lower_bound and rng.random() < 0.2:
            lower = previous
            upper = min(previous + timedelta(minutes=10), REFERENCE_DATETIME)
            timestamp = _random_timestamp(rng, lower, upper)
        else:
            timestamp = _random_timestamp(rng, lower_bound, REFERENCE_DATETIME)
        previous_customer_timestamp[customer_id] = timestamp

        amount = _generate_amount(rng, profile)
        transaction_currency = account.currency if rng.random() < 0.9 else rng.choice(("EUR", "GBP"))

        transactions.append(
            Transaction(
                transaction_id=f"TXN{index:08d}",
                customer_id=customer_id,
                account_id=account.account_id,
                merchant_id=merchant.merchant_id,
                transaction_type=rng.choice(TRANSACTION_TYPES),
                amount=amount,
                currency=transaction_currency,
                timestamp=timestamp,
                country=transaction_country,
                city=transaction_city,
                payment_method=rng.choice(PAYMENT_METHODS),
                status=rng.choices(STATUSES, weights=(0.93, 0.04, 0.03), k=1)[0],
            )
        )

    return transactions


def generate_dataset(config: GeneratorConfig) -> GeneratedDataset:
    """Generate one reproducible, internally related dataset."""
    config.validate()
    rng = random.Random(config.seed)

    customers = generate_customers(rng, config.customers)
    suspicious_count = _choose_customer_count(config.customers, config.suspicious_customer_pct)
    suspicious_customer_ids = frozenset(
        rng.sample([customer.customer_id for customer in customers], k=suspicious_count)
    )
    accounts, accounts_by_customer = generate_accounts(rng, customers, config.accounts)
    merchants = generate_merchants(rng, config.merchants)
    transactions = generate_transactions(
        rng,
        customers,
        accounts_by_customer,
        merchants,
        config.transactions,
        suspicious_customer_ids,
    )

    LOGGER.info(
        "Generated customers=%s accounts=%s merchants=%s transactions=%s suspicious_customers=%s seed=%s",
        len(customers),
        len(accounts),
        len(merchants),
        len(transactions),
        len(suspicious_customer_ids),
        config.seed,
    )

    return GeneratedDataset(
        customers=customers,
        accounts=accounts,
        merchants=merchants,
        transactions=transactions,
        suspicious_customer_ids=suspicious_customer_ids,
    )


def _serialize(value: object) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


def write_dataset_csv(dataset: GeneratedDataset, output_dir: Path) -> None:
    """Write generated entities to deterministic CSV files."""
    output_dir.mkdir(parents=True, exist_ok=True)
    file_specs = (
        ("customers.csv", dataset.customers),
        ("accounts.csv", dataset.accounts),
        ("merchants.csv", dataset.merchants),
        ("transactions.csv", dataset.transactions),
    )

    for filename, rows in file_specs:
        path = output_dir / filename
        field_names = [field.name for field in fields(rows[0])] if rows else []
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(field_names)
            for row in rows:
                writer.writerow([_serialize(getattr(row, field)) for field in field_names])
        LOGGER.info("Wrote %s rows to %s", len(rows), path)
