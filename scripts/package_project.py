"""Create a clean FinBank ZIP only after package validation passes."""

from __future__ import annotations

import argparse
from pathlib import Path
import subprocess
import sys
import zipfile


EXCLUDED_DIRS = {
    ".git",
    ".pytest_cache",
    "__pycache__",
    ".venv",
    "target",
    "dbt_packages",
}

EXCLUDED_ROOTS = {
    "data/generated",
    "data/analytics",
    "data/metrics",
    "data/dashboard",
}


def should_include(path: Path, root: Path) -> bool:
    relative = path.relative_to(root)
    parts = relative.parts

    if any(part in EXCLUDED_DIRS for part in parts):
        return False

    if any(str(relative).replace("\\", "/").startswith(prefix + "/") for prefix in EXCLUDED_ROOTS):
        return False

    return path.is_file()


def main() -> int:
    parser = argparse.ArgumentParser(description="Package FinBank after validation.")
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output ZIP path; defaults to ../finbank-data-platform.zip",
    )
    args = parser.parse_args()

    root = args.root.resolve()
    output = (
        args.output.resolve()
        if args.output is not None
        else root.parent / f"{root.name}.zip"
    )

    validator = root / "scripts" / "validate_package.py"
    result = subprocess.run(
        [sys.executable, str(validator), "--root", str(root)],
        check=False,
    )
    if result.returncode != 0:
        print("Packaging aborted: package validation failed.")
        return result.returncode

    output.parent.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(
        output,
        "w",
        compression=zipfile.ZIP_DEFLATED,
    ) as archive:
        for path in sorted(root.rglob("*")):
            if should_include(path, root):
                archive.write(
                    path,
                    Path(root.name) / path.relative_to(root),
                )

    print(f"Created package: {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
