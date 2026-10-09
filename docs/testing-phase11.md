# FinBank Phase 11 — Testing

## Test strategy

Phase 11 uses a test pyramid with fast deterministic tests at the base, database/dbt checks in the middle, and one explicit live end-to-end verification path at the top.

## Python tests

The Python suite covers:

- deterministic data generation and relational integrity
- configuration boundaries and zero-transaction edge cases
- transaction validation and multi-error rejection
- Kafka event serialization/deserialization
- fraud-rule model contracts and thresholds
- dashboard metric transformations
- monitoring snapshot typing, SQL placeholder alignment, and second-run regression
- workflow dependency/retry/idempotency behavior
- PostgreSQL SQL semantics

Run:

```cmd
pytest -q
```

## Database tests

`tests/test_database_integration.py` verifies against a real PostgreSQL instance:

- insertion into `raw.transaction_events`
- `ON CONFLICT DO NOTHING` duplicate handling
- positive-amount check constraints
- rollback isolation for the test data

It is opt-in so a machine without PostgreSQL does not receive a false integration-test success:

```cmd
set FINBANK_RUN_DB_INTEGRATION_TESTS=1
pytest -q tests\test_database_integration.py
```

## dbt tests

The project includes schema tests using:

- `not_null`
- `unique`
- `relationships`
- `accepted_values`

It also includes singular tests for grain, date continuity, identity consistency, fraud rule evidence, and risk-score bounds.

The standard dbt workflow remains:

```cmd
dbt run --project-dir dbt --profiles-dir %USERPROFILE%\.dbt
dbt test --project-dir dbt --profiles-dir %USERPROFILE%\.dbt
```

## Live end-to-end test

The reproducible runner is:

```cmd
python scripts\run_end_to_end.py
```

It executes the actual path:

```text
Generator
   ↓
Kafka topic (native Kafka)
   ↓
Quality consumer
   ↓
PostgreSQL raw ingestion
   ↓
dbt run + dbt test
   ↓
analytics.fact_transactions / analytics.fact_fraud_alerts
```

The runner creates a unique Kafka topic and unique transaction IDs so it does not depend on prior transaction rows. It verifies that the expected number of generated transactions reach both the raw table and analytical fact table, and reports how many of those transactions generated fraud alerts.

This script is explicitly a **live integration test**. It must not be described as an executed end-to-end success unless the command has actually been run against a live native Kafka/PostgreSQL/dbt environment.

## Final verification sequence

On the canonical Windows machine:

```cmd
pytest -q
set FINBANK_RUN_DB_INTEGRATION_TESTS=1
pytest -q tests\test_database_integration.py
python scripts\run_end_to_end.py
python scripts\collect_metrics.py
python scripts\run_dashboard.py
```

For GitHub, record the real outputs of these commands in the project README rather than hard-coding claims that were not executed.
