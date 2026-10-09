"""Configuration handling for the Phase 1 generator."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


DEFAULT_CUSTOMERS = 1_000
DEFAULT_ACCOUNTS = 1_200
DEFAULT_MERCHANTS = 200
DEFAULT_TRANSACTIONS = 10_000
DEFAULT_SUSPICIOUS_CUSTOMER_PCT = 5.0
DEFAULT_SEED = 42
DEFAULT_OUTPUT_DIR = Path("data/generated")
DEFAULT_LOG_LEVEL = "INFO"


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return default if value is None else int(value)


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    return default if value is None else float(value)


@dataclass(frozen=True)
class GeneratorConfig:
    customers: int = DEFAULT_CUSTOMERS
    accounts: int = DEFAULT_ACCOUNTS
    merchants: int = DEFAULT_MERCHANTS
    transactions: int = DEFAULT_TRANSACTIONS
    suspicious_customer_pct: float = DEFAULT_SUSPICIOUS_CUSTOMER_PCT
    seed: int = DEFAULT_SEED
    output_dir: Path = DEFAULT_OUTPUT_DIR
    log_level: str = DEFAULT_LOG_LEVEL

    @classmethod
    def from_environment(cls) -> "GeneratorConfig":
        config = cls(
            customers=_env_int("FINBANK_CUSTOMERS", DEFAULT_CUSTOMERS),
            accounts=_env_int("FINBANK_ACCOUNTS", DEFAULT_ACCOUNTS),
            merchants=_env_int("FINBANK_MERCHANTS", DEFAULT_MERCHANTS),
            transactions=_env_int("FINBANK_TRANSACTIONS", DEFAULT_TRANSACTIONS),
            suspicious_customer_pct=_env_float(
                "FINBANK_SUSPICIOUS_CUSTOMER_PCT",
                DEFAULT_SUSPICIOUS_CUSTOMER_PCT,
            ),
            seed=_env_int("FINBANK_SEED", DEFAULT_SEED),
            output_dir=Path(os.getenv("FINBANK_OUTPUT_DIR", DEFAULT_OUTPUT_DIR)),
            log_level=os.getenv("FINBANK_LOG_LEVEL", DEFAULT_LOG_LEVEL).upper(),
        )
        config.validate()
        return config

    def validate(self) -> None:
        if self.customers < 1:
            raise ValueError("customers must be at least 1")
        if self.accounts < self.customers:
            raise ValueError("accounts must be >= customers so every customer has an account")
        if self.merchants < 1:
            raise ValueError("merchants must be at least 1")
        if self.transactions < 0:
            raise ValueError("transactions cannot be negative")
        if not 0 <= self.suspicious_customer_pct <= 100:
            raise ValueError("suspicious_customer_pct must be between 0 and 100")
        if self.seed < 0:
            raise ValueError("seed cannot be negative")
        if self.log_level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ValueError("log_level must be DEBUG, INFO, WARNING, ERROR, or CRITICAL")

    def with_overrides(
        self,
        *,
        customers: int | None = None,
        accounts: int | None = None,
        merchants: int | None = None,
        transactions: int | None = None,
        suspicious_customer_pct: float | None = None,
        seed: int | None = None,
        output_dir: Path | None = None,
        log_level: str | None = None,
    ) -> "GeneratorConfig":
        config = GeneratorConfig(
            customers=self.customers if customers is None else customers,
            accounts=self.accounts if accounts is None else accounts,
            merchants=self.merchants if merchants is None else merchants,
            transactions=self.transactions if transactions is None else transactions,
            suspicious_customer_pct=(
                self.suspicious_customer_pct
                if suspicious_customer_pct is None
                else suspicious_customer_pct
            ),
            seed=self.seed if seed is None else seed,
            output_dir=self.output_dir if output_dir is None else output_dir,
            log_level=self.log_level if log_level is None else log_level.upper(),
        )
        config.validate()
        return config
