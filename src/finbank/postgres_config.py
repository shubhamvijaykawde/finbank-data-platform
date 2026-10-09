"""PostgreSQL configuration for FinBank Phase 3."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class PostgresConfig:
    dsn: str
    connect_timeout: int = 5
    retry_attempts: int = 3
    retry_backoff_seconds: float = 1.0

    @classmethod
    def from_environment(cls) -> "PostgresConfig":
        dsn = os.getenv("POSTGRES_DSN", "").strip()
        if not dsn:
            raise ValueError(
                "POSTGRES_DSN is required, for example "
                "postgresql://finbank:change_me@localhost:5432/finbank"
            )

        config = cls(
            dsn=dsn,
            connect_timeout=int(
                os.getenv("POSTGRES_CONNECT_TIMEOUT", "5")
            ),
            retry_attempts=int(
                os.getenv("POSTGRES_RETRY_ATTEMPTS", "3")
            ),
            retry_backoff_seconds=float(
                os.getenv("POSTGRES_RETRY_BACKOFF_SECONDS", "1")
            ),
        )
        config.validate()
        return config

    def validate(self) -> None:
        if not self.dsn:
            raise ValueError("dsn cannot be empty")
        if self.connect_timeout < 1:
            raise ValueError("connect_timeout must be at least 1")
        if self.retry_attempts < 1:
            raise ValueError("retry_attempts must be at least 1")
        if self.retry_backoff_seconds < 0:
            raise ValueError("retry_backoff_seconds cannot be negative")
