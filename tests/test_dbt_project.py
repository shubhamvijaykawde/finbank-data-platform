from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DBT_ROOT = ROOT / "dbt"


def test_dbt_project_file_declares_expected_profile_and_materializations() -> None:
    project = (DBT_ROOT / "dbt_project.yml").read_text(encoding="utf-8")

    assert "name: finbank" in project
    assert "profile: finbank" in project
    assert "require-dbt-version:" in project
    assert "+materialized: view" in project
    assert "+materialized: table" in project


def test_expected_model_files_exist() -> None:
    expected = {
        "staging/stg_customers.sql",
        "staging/stg_accounts.sql",
        "staging/stg_merchants.sql",
        "staging/stg_transactions.sql",
        "intermediate/int_customer_transactions.sql",
        "intermediate/int_daily_transactions.sql",
        "intermediate/int_fraud_candidates.sql",
        "marts/dim_customer.sql",
        "marts/dim_account.sql",
        "marts/dim_merchant.sql",
        "marts/dim_date.sql",
        "marts/fact_transactions.sql",
        "marts/fact_fraud_alerts.sql",
    }

    actual = {
        path.relative_to(DBT_ROOT / "models").as_posix()
        for path in (DBT_ROOT / "models").rglob("*.sql")
    }

    assert expected <= actual


def test_phase7_fraud_alert_model_exists() -> None:
    assert (DBT_ROOT / "models" / "marts" / "fact_fraud_alerts.sql").exists()
    assert (ROOT / "docs" / "fraud-detection-phase7.md").exists()
    assert (ROOT / "sql" / "007_fraud_verification.sql").exists()


def test_profiles_example_uses_environment_variables_for_credentials() -> None:
    profile = (DBT_ROOT / "profiles.yml.example").read_text(encoding="utf-8")

    assert "env_var('DBT_POSTGRES_USER')" in profile
    assert "env_var('DBT_POSTGRES_PASSWORD')" in profile
    assert "password: secret" not in profile.lower()


def test_schema_tests_include_core_assertions() -> None:
    staging = (DBT_ROOT / "models" / "staging" / "staging.yml").read_text(encoding="utf-8")
    marts = (DBT_ROOT / "models" / "marts" / "marts.yml").read_text(encoding="utf-8")

    for text in (staging, marts):
        assert "version: 2" in text
        assert "not_null" in text
        assert "unique" in text

    assert "relationships:" in staging
    assert "relationships:" in marts
    assert "accepted_values:" in staging
    assert "accepted_values:" in marts


def test_phase6_modeling_artifacts_exist() -> None:
    assert (ROOT / "tests" / "test_dbt_build_twice.py").exists()
    assert (ROOT / "docs" / "data-model-phase6.md").exists()
    assert (ROOT / "sql" / "006_data_model_verification.sql").exists()
    assert (DBT_ROOT / "tests" / "test_fact_transaction_grain.sql").exists()
    assert (DBT_ROOT / "tests" / "test_dim_date_contiguous.sql").exists()
    assert (DBT_ROOT / "tests" / "test_dim_account_customer_consistency.sql").exists()


def test_phase6_mart_models_define_physical_primary_or_unique_constraints() -> None:
    models = [
        DBT_ROOT / "models" / "marts" / "dim_customer.sql",
        DBT_ROOT / "models" / "marts" / "dim_account.sql",
        DBT_ROOT / "models" / "marts" / "dim_merchant.sql",
        DBT_ROOT / "models" / "marts" / "dim_date.sql",
        DBT_ROOT / "models" / "marts" / "fact_transactions.sql",
    ]

    for model in models:
        text = model.read_text(encoding="utf-8")
        assert "post_hook" in text
        assert "primary key" in text.lower()
        assert "unique" in text.lower()


def test_phase6_does_not_define_cross_model_physical_foreign_keys() -> None:
    for model in (DBT_ROOT / "models" / "marts").glob("*.sql"):
        text = model.read_text(encoding="utf-8").lower()
        assert " foreign key " not in text


def test_intermediate_customer_transactions_preserves_kafka_provenance() -> None:
    model = (DBT_ROOT / "models" / "intermediate" / "int_customer_transactions.sql").read_text(
        encoding="utf-8"
    )

    for column in (
        "t.kafka_topic",
        "t.kafka_partition",
        "t.kafka_offset",
        "t.ingested_at",
    ):
        assert column in model


def test_mart_constraint_hooks_use_pre_drop_and_post_add() -> None:
    expected_constraints = {
        "dim_customer": (
            "pk_dim_customer",
            "uq_dim_customer_customer_id",
        ),
        "dim_account": (
            "pk_dim_account",
            "uq_dim_account_account_id",
        ),
        "dim_merchant": (
            "pk_dim_merchant",
            "uq_dim_merchant_merchant_id",
        ),
        "dim_date": (
            "pk_dim_date",
            "uq_dim_date_full_date",
        ),
        "fact_transactions": (
            "pk_fact_transactions",
            "uq_fact_transactions_event_id",
            "uq_fact_transactions_transaction_id",
        ),
    }

    for model_name, constraints in expected_constraints.items():
        text = (
            DBT_ROOT / "models" / "marts" / f"{model_name}.sql"
        ).read_text(encoding="utf-8").lower()

        assert "pre_hook=" in text, f"Missing pre_hook for {model_name}"
        assert "post_hook=" in text, f"Missing post_hook for {model_name}"

        pre_section = text.split("pre_hook=", 1)[1].split("post_hook=", 1)[0]
        post_section = text.split("post_hook=", 1)[1]

        for constraint_name in constraints:
            assert (
                f"drop constraint if exists {constraint_name}"
                in pre_section
            ), f"Missing pre-hook DROP for {constraint_name}"
            assert (
                f"add constraint {constraint_name}"
                in post_section
            ), f"Missing post-hook ADD for {constraint_name}"


def test_phase7_fraud_candidates_define_expected_rules_and_score() -> None:
    model = (DBT_ROOT / "models" / "intermediate" / "int_fraud_candidates.sql").read_text(
        encoding="utf-8"
    ).lower()

    for expected in (
        "large_transaction_flag",
        "transaction_burst_flag",
        "impossible_travel_flag",
        "unusual_spending_flag",
        "failed_attempts_before_large_success_flag",
        "suspicious_merchant_behavior_flag",
        "risk_score",
        "least(\n            100",
    ):
        assert expected in model


def test_phase7_alert_fact_is_one_row_per_transaction() -> None:
    model = (DBT_ROOT / "models" / "marts" / "fact_fraud_alerts.sql").read_text(
        encoding="utf-8"
    ).lower()

    assert "where c.risk_score > 0" in model
    assert "unique (transaction_id)" in model
    assert "rule_triggered" in model
    assert "reason" in model
