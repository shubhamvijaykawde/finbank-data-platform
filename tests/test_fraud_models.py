from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DBT_ROOT = ROOT / "dbt"


def test_fraud_candidates_contain_all_six_rule_flags() -> None:
    text = (
        DBT_ROOT / "models" / "intermediate" / "int_fraud_candidates.sql"
    ).read_text(encoding="utf-8")

    expected_flags = (
        "large_transaction_flag",
        "transaction_burst_flag",
        "impossible_travel_flag",
        "unusual_spending_flag",
        "failed_attempts_before_large_success_flag",
        "suspicious_merchant_behavior_flag",
    )

    for flag in expected_flags:
        assert flag in text


def test_fraud_candidates_define_expected_thresholds() -> None:
    text = (
        DBT_ROOT / "models" / "intermediate" / "int_fraud_candidates.sql"
    ).read_text(encoding="utf-8")

    for threshold in (
        "3000.00",
        "10.0",
        "60.0",
        "5.0",
        "1500.00",
        "30.0",
        "2000.00",
    ):
        assert threshold in text


def test_fraud_alert_model_preserves_explanations_and_provenance() -> None:
    text = (
        DBT_ROOT / "models" / "marts" / "fact_fraud_alerts.sql"
    ).read_text(encoding="utf-8")

    for column in (
        "fraud_alert_id",
        "transaction_id",
        "customer_id",
        "rule_triggered",
        "risk_score",
        "severity",
        "reason",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "created_at",
    ):
        assert column in text


def test_fraud_alert_model_filters_to_triggered_transactions() -> None:
    text = (
        DBT_ROOT / "models" / "marts" / "fact_fraud_alerts.sql"
    ).read_text(encoding="utf-8").lower()

    assert "where c.risk_score > 0" in text
    assert "unique (transaction_id)" in text
