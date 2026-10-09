"""Launch the FinBank Streamlit dashboard."""

from __future__ import annotations

import argparse
from pathlib import Path
import shutil
import subprocess
import sys

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
APP_PATH = REPOSITORY_ROOT / "dashboard" / "app.py"


def main() -> int:
    parser = argparse.ArgumentParser(description="Launch the FinBank Streamlit dashboard.")
    parser.add_argument("--port", type=int, default=8501)
    args = parser.parse_args()

    if not 1 <= args.port <= 65535:
        raise SystemExit("--port must be between 1 and 65535")

    streamlit = shutil.which("streamlit")
    command = [streamlit, "run", str(APP_PATH), "--server.port", str(args.port)] if streamlit else [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(APP_PATH),
        "--server.port",
        str(args.port),
    ]

    try:
        return subprocess.call(command, cwd=REPOSITORY_ROOT)
    except FileNotFoundError:
        print("Streamlit is not installed. Run: pip install -r requirements.txt")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
