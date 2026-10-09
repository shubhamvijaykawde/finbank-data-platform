"""Run the FinBank Phase 8 lightweight workflow."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess
import json
import time
from datetime import datetime, timezone
import sys

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPOSITORY_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

from finbank.analytics_report import generate_analytics_report
from finbank.logging_config import configure_logging
from finbank.postgres_config import PostgresConfig
from finbank.operational_events import record_operational_event
from finbank.workflow import Task, run_workflow


def _run_command(command: list[str]) -> None:
    result = subprocess.run(
        command,
        cwd=REPOSITORY_ROOT,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Command failed with exit code {result.returncode}: "
            + " ".join(command)
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the FinBank lightweight Phase 8 workflow."
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=int(os.getenv("FINBANK_PIPELINE_RETRIES", "2")),
        help="Retries after the initial task attempt.",
    )
    parser.add_argument(
        "--retry-delay-seconds",
        type=float,
        default=float(os.getenv("FINBANK_PIPELINE_RETRY_DELAY_SECONDS", "2")),
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--log-level", default=os.getenv("FINBANK_LOG_LEVEL", "INFO"))
    parser.add_argument("--dbt-project-dir", type=Path, default=Path("dbt"))
    parser.add_argument(
        "--dbt-profiles-dir",
        type=Path,
        default=Path.home() / ".dbt",
    )
    parser.add_argument(
        "--analytics-output",
        type=Path,
        default=Path("data/analytics/latest_report.json"),
    )
    args = parser.parse_args()

    if args.retries < 0:
        raise SystemExit("--retries cannot be negative")
    if args.retry_delay_seconds < 0:
        raise SystemExit("--retry-delay-seconds cannot be negative")

    configure_logging(args.log_level)

    project_dir = (
        REPOSITORY_ROOT / args.dbt_project_dir
        if not args.dbt_project_dir.is_absolute()
        else args.dbt_project_dir
    )
    profiles_dir = (
        REPOSITORY_ROOT / args.dbt_profiles_dir
        if not args.dbt_profiles_dir.is_absolute()
        else args.dbt_profiles_dir
    )
    analytics_output = (
        REPOSITORY_ROOT / args.analytics_output
        if not args.analytics_output.is_absolute()
        else args.analytics_output
    )

    def validate() -> None:
        _run_command([sys.executable, str(REPOSITORY_ROOT / "scripts" / "validate_pipeline.py")])

    def dbt_run() -> None:
        _run_command(
            [
                "dbt",
                "run",
                "--project-dir",
                str(project_dir),
                "--profiles-dir",
                str(profiles_dir),
            ]
        )

    def dbt_test() -> None:
        _run_command(
            [
                "dbt",
                "test",
                "--project-dir",
                str(project_dir),
                "--profiles-dir",
                str(profiles_dir),
            ]
        )

    def generate_analytics() -> None:
        config = PostgresConfig.from_environment()
        generate_analytics_report(config, analytics_output)

    tasks = [
        Task("validate", validate),
        Task("dbt_run", dbt_run, ("validate",)),
        Task("dbt_test", dbt_test, ("dbt_run",)),
        Task("generate_analytics", generate_analytics, ("dbt_test",)),
    ]

    pipeline_started_at = datetime.now(timezone.utc)
    monotonic_started = time.monotonic()
    status = "success"
    error_message: str | None = None

    try:
        run_workflow(
            tasks,
            max_retries=args.retries,
            retry_delay_seconds=args.retry_delay_seconds,
            dry_run=args.dry_run,
        )
    except Exception as exc:  # noqa: BLE001 - command-line boundary
        status = "failed"
        error_message = str(exc)
        print(f"Pipeline failed: {exc}")
    finally:
        elapsed_seconds = round(time.monotonic() - monotonic_started, 6)
        if not args.dry_run:
            metrics_dir = REPOSITORY_ROOT / "data" / "metrics"
            metrics_dir.mkdir(parents=True, exist_ok=True)
            pipeline_record = {
                "started_at_utc": pipeline_started_at.isoformat(),
                "finished_at_utc": datetime.now(timezone.utc).isoformat(),
                "elapsed_seconds": elapsed_seconds,
                "status": status,
            }
            if error_message is not None:
                pipeline_record["error"] = error_message
            pipeline_path = metrics_dir / "latest_pipeline_run.json"
            temporary_path = metrics_dir / ".latest_pipeline_run.tmp"
            temporary_path.write_text(
                json.dumps(pipeline_record, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            temporary_path.replace(pipeline_path)
            record_operational_event(
                "pipeline_run",
                "pipeline",
                status=status,
                elapsed_seconds=elapsed_seconds,
            )

    if status != "success":
        return 1

    print("FinBank pipeline completed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
