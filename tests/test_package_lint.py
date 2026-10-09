from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
VALIDATOR_PATH = ROOT / "scripts" / "validate_package.py"

spec = spec_from_file_location("finbank_validate_package", VALIDATOR_PATH)
assert spec is not None and spec.loader is not None
validator = module_from_spec(spec)
spec.loader.exec_module(validator)

validate_repository = validator.validate_repository


def test_repository_passes_package_validation() -> None:
    assert validate_repository(ROOT) == []


def test_package_validator_rejects_yaml_inside_sql(tmp_path: Path) -> None:
    (tmp_path / "sql").mkdir()
    (tmp_path / "dbt" / "tests").mkdir(parents=True)
    (tmp_path / "sql" / "bad.sql").write_text(
        "version: 2\nSELECT 1;\n",
        encoding="utf-8",
    )
    (tmp_path / "dbt" / "tests" / "good.sql").write_text(
        "select 1 from {{ ref('model') }};\n",
        encoding="utf-8",
    )

    errors = validate_repository(tmp_path)

    assert any("version:" in error for error in errors)


def test_package_validator_rejects_singular_test_without_ref_or_source(
    tmp_path: Path,
) -> None:
    (tmp_path / "sql").mkdir()
    (tmp_path / "dbt" / "tests").mkdir(parents=True)
    (tmp_path / "sql" / "good.sql").write_text(
        "-- valid SQL\nSELECT 1;\n",
        encoding="utf-8",
    )
    (tmp_path / "dbt" / "tests" / "bad.sql").write_text(
        "select * from model;\n",
        encoding="utf-8",
    )

    errors = validate_repository(tmp_path)

    assert any("must reference" in error for error in errors)


def test_package_validator_rejects_markdown_prose_inside_plain_sql(
    tmp_path: Path,
) -> None:
    (tmp_path / "sql").mkdir()
    (tmp_path / "dbt" / "tests").mkdir(parents=True)
    (tmp_path / "sql" / "bad.sql").write_text(
        "-- invalid SQL\namount is additive only within one currency\n",
        encoding="utf-8",
    )
    (tmp_path / "dbt" / "tests" / "good.sql").write_text(
        "select 1 from {{ ref('model') }};\n",
        encoding="utf-8",
    )

    errors = validate_repository(tmp_path)

    assert any("possible markdown prose" in error for error in errors)
