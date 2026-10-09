from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]
DBT_ROOT = ROOT / "dbt"


@pytest.mark.skipif(
    os.getenv("FINBANK_RUN_DBT_INTEGRATION_TESTS") != "1",
    reason=(
        "Live dbt/PostgreSQL integration test is disabled. Set "
        "FINBANK_RUN_DBT_INTEGRATION_TESTS=1 to run it."
    ),
)
def test_dbt_build_succeeds_twice() -> None:
    """Regression test for idempotent mart constraint hooks."""
    dbt_executable = shutil.which("dbt")
    if dbt_executable is None:
        pytest.fail("dbt executable not found on PATH")

    required_env = (
        "DBT_POSTGRES_HOST",
        "DBT_POSTGRES_PORT",
        "DBT_POSTGRES_DB",
        "DBT_POSTGRES_USER",
        "DBT_POSTGRES_PASSWORD",
    )
    missing = [name for name in required_env if not os.getenv(name)]
    if missing:
        pytest.fail(
            "Missing dbt PostgreSQL environment variables: "
            + ", ".join(missing)
        )

    profiles_dir = Path.home() / ".dbt"
    profiles_file = profiles_dir / "profiles.yml"
    if not profiles_file.exists():
        pytest.fail(f"dbt profile not found: {profiles_file}")

    command = [
        dbt_executable,
        "build",
        "--project-dir",
        str(DBT_ROOT),
        "--profiles-dir",
        str(profiles_dir),
    ]

    for run_number in (1, 2):
        result = subprocess.run(
            command,
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )

        if result.returncode != 0:
            pytest.fail(
                f"dbt build #{run_number} failed with exit code "
                f"{result.returncode}.\n\nSTDOUT:\n"
                f"{result.stdout}\n\nSTDERR:\n{result.stderr}"
            )
