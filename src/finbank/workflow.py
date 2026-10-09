"""Lightweight dependency-aware workflow runner for FinBank."""

from __future__ import annotations

from dataclasses import dataclass
import logging
import time
from collections.abc import Callable, Mapping, Sequence

LOGGER = logging.getLogger(__name__)


TaskAction = Callable[[], None]


@dataclass(frozen=True)
class Task:
    """One workflow task with explicit dependencies."""

    name: str
    action: TaskAction
    dependencies: tuple[str, ...] = ()


class WorkflowError(RuntimeError):
    """Raised when a workflow cannot be constructed or completed."""


def resolve_execution_order(tasks: Sequence[Task]) -> list[str]:
    """Return a deterministic topological ordering of workflow tasks."""
    task_map: Mapping[str, Task] = {task.name: task for task in tasks}

    if len(task_map) != len(tasks):
        raise WorkflowError("Task names must be unique")

    order: list[str] = []
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(name: str) -> None:
        if name in visited:
            return
        if name in visiting:
            raise WorkflowError(f"Dependency cycle detected at task '{name}'")
        if name not in task_map:
            raise WorkflowError(f"Unknown task dependency: '{name}'")

        visiting.add(name)
        for dependency in task_map[name].dependencies:
            visit(dependency)
        visiting.remove(name)
        visited.add(name)
        order.append(name)

    for task in tasks:
        visit(task.name)

    return order


def run_workflow(
    tasks: Sequence[Task],
    *,
    max_retries: int = 2,
    retry_delay_seconds: float = 2.0,
    dry_run: bool = False,
) -> None:
    """Run tasks in dependency order with bounded retries.

    ``max_retries`` is the number of retries after the initial attempt.
    Therefore a value of 0 means exactly one attempt.
    """
    if max_retries < 0:
        raise ValueError("max_retries cannot be negative")
    if retry_delay_seconds < 0:
        raise ValueError("retry_delay_seconds cannot be negative")

    order = resolve_execution_order(tasks)
    task_map = {task.name: task for task in tasks}

    LOGGER.info("Workflow order: %s", " -> ".join(order))

    if dry_run:
        for index, name in enumerate(order, start=1):
            task = task_map[name]
            LOGGER.info(
                "DRY RUN task=%s position=%s dependencies=%s",
                task.name,
                index,
                list(task.dependencies),
            )
        return

    completed: set[str] = set()

    for name in order:
        task = task_map[name]

        missing = set(task.dependencies) - completed
        if missing:
            raise WorkflowError(
                f"Task '{name}' cannot run; dependencies not completed: "
                + ", ".join(sorted(missing))
            )

        attempts = max_retries + 1
        last_error: Exception | None = None

        for attempt in range(1, attempts + 1):
            started = time.monotonic()
            LOGGER.info(
                "Task started: name=%s attempt=%s/%s",
                task.name,
                attempt,
                attempts,
            )

            try:
                task.action()
            except Exception as exc:  # noqa: BLE001 - workflow boundary
                last_error = exc
                elapsed = time.monotonic() - started
                LOGGER.error(
                    "Task failed: name=%s attempt=%s/%s "
                    "elapsed_seconds=%.3f error=%s",
                    task.name,
                    attempt,
                    attempts,
                    elapsed,
                    exc,
                )

                if attempt == attempts:
                    break

                delay = retry_delay_seconds * (2 ** (attempt - 1))
                LOGGER.warning(
                    "Retrying task: name=%s next_attempt=%s "
                    "delay_seconds=%.3f",
                    task.name,
                    attempt + 1,
                    delay,
                )
                if delay:
                    time.sleep(delay)
                continue

            elapsed = time.monotonic() - started
            completed.add(task.name)
            LOGGER.info(
                "Task completed: name=%s elapsed_seconds=%.3f",
                task.name,
                elapsed,
            )
            break

        else:  # pragma: no cover - defensive guard
            raise WorkflowError(f"Task '{task.name}' did not execute")

        if task.name not in completed:
            raise WorkflowError(
                f"Task '{task.name}' failed after {attempts} attempt(s)"
            ) from last_error
