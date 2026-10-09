"""Repository entry point for the Phase 1 generator."""

from pathlib import Path
import sys

# Make the src-layout package runnable directly from a fresh checkout without
# requiring an editable install first. This keeps local setup simple.
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPOSITORY_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

from finbank.cli import main


if __name__ == "__main__":
    raise SystemExit(main())
