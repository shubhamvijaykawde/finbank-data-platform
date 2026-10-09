"""Logging configuration for FinBank command-line jobs."""

import logging


def configure_logging(level: str) -> None:
    """Configure one predictable console logger for local jobs."""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
