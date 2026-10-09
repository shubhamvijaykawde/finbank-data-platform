"""Command-line interface for Phase 1 data generation."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from finbank.config import GeneratorConfig
from finbank.generator import generate_dataset, write_dataset_csv
from finbank.logging_config import configure_logging

LOGGER = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate reproducible FinBank banking data.")
    parser.add_argument("--customers", type=int, default=None)
    parser.add_argument("--accounts", type=int, default=None)
    parser.add_argument("--merchants", type=int, default=None)
    parser.add_argument("--transactions", type=int, default=None)
    parser.add_argument("--suspicious-customer-pct", type=float, default=None)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--log-level", type=str, default=None)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        config = GeneratorConfig.from_environment().with_overrides(
            customers=args.customers,
            accounts=args.accounts,
            merchants=args.merchants,
            transactions=args.transactions,
            suspicious_customer_pct=args.suspicious_customer_pct,
            seed=args.seed,
            output_dir=args.output_dir,
            log_level=args.log_level,
        )
    except ValueError as exc:
        print(f"Configuration error: {exc}")
        return 2

    configure_logging(config.log_level)
    dataset = generate_dataset(config)
    write_dataset_csv(dataset, config.output_dir)
    LOGGER.info("Generation complete. Output directory: %s", config.output_dir)
    return 0
