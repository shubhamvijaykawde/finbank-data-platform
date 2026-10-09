from __future__ import annotations

from finbank.workflow import Task, WorkflowError, resolve_execution_order, run_workflow


def test_workflow_resolves_dependency_order() -> None:
    completed: list[str] = []

    tasks = [
        Task("analytics", lambda: completed.append("analytics"), ("tests",)),
        Task("tests", lambda: completed.append("tests"), ("dbt",)),
        Task("dbt", lambda: completed.append("dbt"), ("validate",)),
        Task("validate", lambda: completed.append("validate")),
    ]

    assert resolve_execution_order(tasks) == [
        "validate",
        "dbt",
        "tests",
        "analytics",
    ]


def test_workflow_rejects_unknown_dependency() -> None:
    try:
        resolve_execution_order([Task("dbt", lambda: None, ("missing",))])
    except WorkflowError as exc:
        assert "Unknown task dependency" in str(exc)
    else:
        raise AssertionError("Expected WorkflowError")


def test_workflow_rejects_cycles() -> None:
    tasks = [
        Task("a", lambda: None, ("b",)),
        Task("b", lambda: None, ("a",)),
    ]

    try:
        resolve_execution_order(tasks)
    except WorkflowError as exc:
        assert "Dependency cycle" in str(exc)
    else:
        raise AssertionError("Expected WorkflowError")


def test_workflow_retries_failed_task_until_success() -> None:
    attempts = 0
    completed: list[str] = []

    def flaky() -> None:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise RuntimeError("transient failure")
        completed.append("flaky")

    run_workflow(
        [Task("flaky", flaky)],
        max_retries=2,
        retry_delay_seconds=0,
    )

    assert attempts == 3
    assert completed == ["flaky"]


def test_workflow_does_not_run_dependents_after_failure() -> None:
    executed: list[str] = []

    def fail() -> None:
        executed.append("fail")
        raise RuntimeError("permanent failure")

    tasks = [
        Task("fail", fail),
        Task("dependent", lambda: executed.append("dependent"), ("fail",)),
    ]

    try:
        run_workflow(
            tasks,
            max_retries=0,
            retry_delay_seconds=0,
        )
    except WorkflowError:
        pass
    else:
        raise AssertionError("Expected WorkflowError")

    assert executed == ["fail"]
