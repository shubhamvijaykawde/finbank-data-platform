from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_phase8_workflow_artifacts_exist() -> None:
    for relative_path in (
        "src/finbank/workflow.py",
        "src/finbank/analytics_report.py",
        "scripts/validate_pipeline.py",
        "scripts/generate_analytics.py",
        "scripts/run_pipeline.py",
        "docs/lightweight-orchestration-phase8.md",
    ):
        assert (ROOT / relative_path).exists()


def test_run_pipeline_has_expected_task_graph() -> None:
    text = (ROOT / "scripts" / "run_pipeline.py").read_text(encoding="utf-8")

    assert 'Task("validate", validate)' in text
    assert 'Task("dbt_run", dbt_run, ("validate",))' in text
    assert 'Task("dbt_test", dbt_test, ("dbt_run",))' in text
    assert 'Task("generate_analytics", generate_analytics, ("dbt_test",))' in text


def test_phase8_does_not_introduce_airflow() -> None:
    for relative_path in (
        "requirements.txt",
        "scripts/run_pipeline.py",
        "src/finbank/workflow.py",
    ):
        text = (ROOT / relative_path).read_text(encoding="utf-8").lower()
        assert "airflow" not in text
