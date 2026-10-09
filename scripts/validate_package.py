"""Validate FinBank source files before packaging a repository archive."""

from __future__ import annotations

import argparse
from pathlib import Path
import re
import sys


PLAIN_SQL_PREFIXES = ("--", "select", "with")
SQL_KEYWORDS = {
    "select", "with", "from", "where", "and", "or", "on", "join", "left",
    "right", "inner", "outer", "full", "group", "order", "having", "union",
    "all", "case", "when", "then", "else", "end", "as", "into", "values",
    "insert", "update", "delete", "create", "alter", "drop", "do", "begin",
    "commit", "rollback", "grant", "revoke", "comment", "call", "table",
    "constraint", "references", "returning", "using", "cross", "distinct",
}


def first_significant_line(text: str) -> str:
    for line in text.lstrip("\ufeff").splitlines():
        stripped = line.strip()
        if stripped:
            return stripped
    return ""


def lint_plain_sql_file(path: Path) -> list[str]:
    errors: list[str] = []
    text = path.read_text(encoding="utf-8")
    first = first_significant_line(text).lower()

    # Pure SQL files under sql/ must begin with a SQL comment, SELECT, or WITH.
    if first and not first.startswith(PLAIN_SQL_PREFIXES):
        errors.append(
            f"{path}: first significant line must begin with --, SELECT, or WITH"
        )

    for line_no, line in enumerate(text.splitlines(), start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue

        if re.match(r"^version:\s*", stripped, re.IGNORECASE):
            errors.append(f"{path}:{line_no}: YAML marker 'version:' found in SQL")
        if re.match(r"^models:\s*", stripped, re.IGNORECASE):
            errors.append(f"{path}:{line_no}: YAML marker 'models:' found in SQL")

        # Catch plain markdown fragments accidentally pasted into SQL.
        lowered = stripped.lower()
        first_word = re.match(r"^([a-z_]+)", lowered)
        if first_word:
            keyword = first_word.group(1)
            if keyword not in SQL_KEYWORDS and re.search(r"\b(is|require|only)\b", lowered):
                errors.append(
                    f"{path}:{line_no}: possible markdown prose in SQL: {stripped}"
                )
            if "-" in stripped and re.search(r"\b(require|only|is)\b", lowered):
                errors.append(
                    f"{path}:{line_no}: possible markdown prose in SQL: {stripped}"
                )

    return errors


def lint_dbt_models(path: Path) -> list[str]:
    errors: list[str] = []
    text = path.read_text(encoding="utf-8")
    # dbt model SQL may intentionally start with {{ config(...) }}.
    if not first_significant_line(text).startswith(("{{", "--", "select", "with")):
        errors.append(
            f"{path}: dbt model must begin with a comment, SELECT/WITH, or a Jinja config block"
        )

    if re.search(r"^version:\s*", text, re.MULTILINE | re.IGNORECASE):
        errors.append(f"{path}: YAML marker 'version:' found in SQL")
    if re.search(r"^models:\s*", text, re.MULTILINE | re.IGNORECASE):
        errors.append(f"{path}: YAML marker 'models:' found in SQL")

    return errors


def lint_dbt_tests(root: Path) -> list[str]:
    errors: list[str] = []
    for path in sorted((root / "dbt" / "tests").glob("*.sql")):
        text = path.read_text(encoding="utf-8")
        if not re.search(r"\{\{\s*(ref|source)\s*\(", text):
            errors.append(
                f"{path}: dbt singular test must reference {{ ref(...) }} or {{ source(...) }}"
            )
    return errors


def validate_repository(root: Path) -> list[str]:
    errors: list[str] = []

    sql_root = root / "sql"
    if sql_root.exists():
        for path in sorted(sql_root.glob("*.sql")):
            errors.extend(lint_plain_sql_file(path))

    dbt_models = root / "dbt" / "models"
    if dbt_models.exists():
        for path in sorted(dbt_models.rglob("*.sql")):
            errors.extend(lint_dbt_models(path))

    errors.extend(lint_dbt_tests(root))

    return errors


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate FinBank source files before creating a ZIP archive."
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="FinBank repository root",
    )
    args = parser.parse_args()

    errors = validate_repository(args.root.resolve())

    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        print(f"Package validation failed with {len(errors)} error(s).")
        return 1

    print("Package validation passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
