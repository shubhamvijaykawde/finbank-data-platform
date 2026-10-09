from __future__ import annotations

from pathlib import Path


def test_phase11_end_to_end_runner_exists() -> None:
    root = Path(__file__).resolve().parents[1]
    script = (root / "scripts" / "run_end_to_end.py").read_text(encoding="utf-8")

    for stage in (
        "generate_data.py",
        "create_kafka_topic.py",
        "load_reference_data.py",
        "produce_transactions.py",
        "consume_transactions_with_quality.py",
        "run_pipeline.py",
    ):
        assert stage in script


def test_phase11_end_to_end_is_explicitly_live_only() -> None:
    root = Path(__file__).resolve().parents[1]
    script = (root / "scripts" / "run_end_to_end.py").read_text(encoding="utf-8").lower()
    assert "not a mocked test" in script
    assert "localhost:9092" in script or "bootstrap" in script
