"""Lightweight local operational event journal for FinBank."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

DEFAULT_EVENT_LOG = Path("data/metrics/operational_events.jsonl")


def _resolve_path(path: Path) -> Path:
    if path.is_absolute():
        return path
    return Path(__file__).resolve().parents[2] / path


def record_operational_event(
    event_type: str,
    component: str,
    *,
    message: str | None = None,
    log_path: Path = DEFAULT_EVENT_LOG,
    **fields: Any,
) -> None:
    """Append one structured operational event to a local JSONL journal."""
    path = _resolve_path(log_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    event: dict[str, Any] = {
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "event_type": event_type,
        "component": component,
    }
    if message is not None:
        event["message"] = message
    event.update(fields)

    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, sort_keys=True) + "\n")


def read_operational_events(log_path: Path = DEFAULT_EVENT_LOG) -> list[dict[str, Any]]:
    """Read valid structured events; ignore a trailing partial JSON line."""
    path = _resolve_path(log_path)
    if not path.exists():
        return []

    events: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            try:
                value = json.loads(stripped)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                events.append(value)
    return events
