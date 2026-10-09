# FinBank Phase 7 — Explainable Fraud Detection

## Purpose

Phase 7 adds a deterministic, rule-based fraud-analysis layer on top of the Phase 6 analytical model.

The goal is explainability and analytical engineering, not production fraud detection. No machine-learning system or additional infrastructure is introduced.

## Data flow

```text
analytics.fact_transactions
            |
            v
staging.int_customer_transactions
            |
            v
staging.int_fraud_candidates
            |
            v
analytics.fact_fraud_alerts
```

## Candidate model

`staging.int_fraud_candidates` evaluates six independent rule flags for each valid transaction and computes an additive score capped at 100.

The candidate model exposes the rule flags, their supporting features, the risk score, severity, and a human-readable reason.

## Rules

### Large transaction

```text
amount > 3,000
```

Weight: `40`

### Transaction burst

The previous customer transaction occurred within `10` minutes.

Weight: `20`

### Impossible travel

The transaction country differs from the previous transaction country and the previous transaction occurred within `60` minutes.

Weight: `35`

This is only a country-level heuristic; it is not a true geographic-distance calculation.

### Unusual spending

```text
amount >= 5x prior customer average
```

Weight: `25`

The first transaction for a customer has no prior average and therefore does not trigger this rule.

### Failed attempts before a large success

A completed transaction of at least `1,500` follows two consecutive failed transactions within the `30` minute window.

Weight: `30`

### Suspicious merchant behavior

A transaction of at least `2,000` occurs in one of the monitored categories:

```text
electronics
travel
entertainment
```

Weight: `15`

## Scoring

Rule weights are additive and capped at `100`:

```text
risk_score = min(100, sum(triggered rule weights))
```

Severity is derived as:

```text
HIGH    >= 70
MEDIUM  40-69
LOW     1-39
```

A zero score is not materialized in the alert fact.

## Alert grain

`analytics.fact_fraud_alerts` has a grain of:

> one row per alerted transaction.

A transaction may trigger multiple rules, but those rules are aggregated into one alert row.

The fields are:

```text
fraud_alert_id
transaction_id
customer_id
rule_triggered
risk_score
severity
reason
amount
currency
transaction_timestamp
transaction_country
merchant_category
kafka_topic
kafka_partition
kafka_offset
created_at
```

## Multiple-rule aggregation

`rule_triggered` contains semicolon-separated rule codes.

`reason` contains semicolon-separated human-readable explanations in the same rule order.

For example, one alert can contain:

```text
rule_triggered:
LARGE_TRANSACTION; SUSPICIOUS_MERCHANT_BEHAVIOR
```

while retaining both explanations.

There is still only one alert row for the transaction.

## Determinism

The fraud model uses deterministic SQL rules and does not use random scoring.

Rebuilding the model over the same source facts therefore produces the same analytical result.

## Verification

Run:

```cmd
dbt build --project-dir dbt --profiles-dir "%USERPROFILE%\.dbt"
```

Then:

```cmd
psql -U finbank -h localhost -d finbank -f sql\007_fraud_verification.sql
```

The verification script checks:

- alert count
- severity distribution
- rule frequency
- explanation integrity
- duplicate alert IDs and transaction IDs
- orphan alerts
- risk-score bounds
- severity/score consistency

## Tests

The Phase 7 dbt singular tests include:

```text
test_fraud_alert_grain.sql
test_fraud_alert_scores_are_positive.sql
test_fraud_alerts_have_rule_evidence.sql
test_fraud_risk_score_bounds.sql
```

The rule-evidence test fails if `rule_triggered` or `reason` is null or empty.

## Limitations

These rules are analytical heuristics for a portfolio project.

They are not calibrated against a labeled fraud dataset and are not suitable as production banking controls.

The implementation does not provide:

- calibrated probability of fraud
- true geographic-distance calculations
- customer-level device intelligence
- account takeover signals
- merchant-risk models
- chargeback outcomes
- supervised machine learning
- real-time model serving
- FX normalization across currencies

Those limitations are intentional and should be stated during interviews.

## Phase boundary

Phase 7 ends with deterministic fraud candidates and the `fact_fraud_alerts` analytical mart.

Workflow orchestration, operational metrics, and dashboarding remain later phases.

## Phase 7 file checklist

```text
dbt/models/intermediate/int_fraud_candidates.sql
dbt/models/marts/fact_fraud_alerts.sql
dbt/models/marts/marts.yml
dbt/tests/test_fraud_alert_grain.sql
dbt/tests/test_fraud_alert_scores_are_positive.sql
dbt/tests/test_fraud_alerts_have_rule_evidence.sql
dbt/tests/test_fraud_risk_score_bounds.sql
sql/007_fraud_verification.sql
tests/test_fraud_models.py
docs/fraud-detection-phase7.md
```
