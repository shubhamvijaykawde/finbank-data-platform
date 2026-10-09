from __future__ import annotations

import pytest

from finbank.config import GeneratorConfig
from finbank.generator import generate_dataset


def test_generator_allows_zero_transactions_for_reference_only_runs() -> None:
    dataset = generate_dataset(
        GeneratorConfig(
            customers=5,
            accounts=5,
            merchants=1,
            transactions=0,
            suspicious_customer_pct=0,
            seed=1,
        )
    )
    assert len(dataset.transactions) == 0
    assert len(dataset.customers) == 5


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("customers", 0),
        ("accounts", 4),
        ("merchants", 0),
        ("transactions", -1),
        ("suspicious_customer_pct", -0.1),
        ("suspicious_customer_pct", 100.1),
        ("seed", -1),
    ],
)
def test_generator_rejects_invalid_boundaries(field: str, value: object) -> None:
    values = dict(
        customers=5,
        accounts=5,
        merchants=1,
        transactions=10,
        suspicious_customer_pct=5.0,
        seed=1,
    )
    values[field] = value
    with pytest.raises(ValueError):
        GeneratorConfig(**values).validate()
