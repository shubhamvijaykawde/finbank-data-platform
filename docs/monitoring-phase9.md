# FinBank Phase 9 — Lightweight Operational Monitoring

## Scope

Phase 9 adds a small operational metrics layer using Python standard-library code plus the already-installed Psycopg dependency. It stores point-in-time snapshots in PostgreSQL and writes a local JSON snapshot for inspection.

The phase captures the operational signals required by the FinBank roadmap: generated transactions, processed transactions, rejected records, observed transaction throughput, fraud alerts, high-risk transactions, consumer errors, database errors, and pipeline execution time.

## Non-goals

This phase does not add Prometheus, Grafana, Elasticsearch, Kafka Connect, a monitoring server, cloud telemetry, alert-routing infrastructure, or another always-running service. It does not change existing dbt models. It does not alter Kafka topic/partition design.

## Environment

Canonical development environment:

```text
Windows 10 Enterprise LTSC 2019
Kafka 3.9.2 — native Windows, KRaft, localhost:9092
PostgreSQL 17.2 — native Windows, localhost:5432
dbt Core 1.12.5
dbt-postgres 1.11.0
Python 3.14.8
CMD shell
```

No Docker or WSL2 is required.

## PostgreSQL interaction

Phase 9 adds a separate `monitoring` schema with `monitoring.metric_snapshots`. The collector reads existing relations only:

```text
raw.transaction_events
raw.rejected_records
analytics.fact_fraud_alerts
```

It also reads the local Phase 1 transaction CSV and Phase 8 pipeline-run JSON file. Existing `staging.*` and `analytics.*` dbt model SQL is not changed.

The metrics table is initialized by the existing `scripts/init_database.py` command through `sql/008_create_monitoring.sql`.

## Kafka interaction

Phase 9 does not create or modify Kafka topics, partitions, offsets, consumer groups, or retention settings. Existing Phase 2–4 consumers emit lightweight local operational events when a consumer error occurs and emit an ingestion-run event when a run completes. These events are written to `data/metrics/operational_events.jsonl`.

No Kafka broker connection is needed by `scripts/collect_metrics.py`; it reads the local event journal. This keeps metrics collection independent of the broker's runtime state.

## Pipeline interaction

The Phase 8 runner now records the latest pipeline duration/status in:

```text
data/metrics/latest_pipeline_run.json
```

The metrics collector reads that file and exposes `pipeline_execution_time_seconds`. Recording is best-effort and does not turn metrics persistence into an additional required workflow task.

## Metric semantics

### `transactions_generated`

Current row count in `data/generated/transactions.csv`. This represents the generated source dataset, not Kafka publication count.

### `transactions_processed`

Current count of valid rows in `raw.transaction_events`.

### `transactions_rejected`

Current count of rows in `raw.rejected_records`.

### `transactions_per_minute`

Observed throughput for ingestion-run events recorded since the previous metric snapshot: total processed messages divided by the recorded ingestion runtime in minutes. If no ingestion run exists in the interval, the value is `0`. This is an observed local batch/consumer rate, not a benchmark.

### `fraud_alerts` / `high_risk_transactions`

Current counts from `analytics.fact_fraud_alerts`; no fraud logic is changed in Phase 9.

### `consumer_errors` / `database_errors`

Counts of structured error events since the previous metric snapshot. The first snapshot counts all parseable existing error events in the local journal. A database outage can prevent a database-backed metric snapshot from being persisted; local operational events remain the fallback record for those failures.

### `pipeline_execution_time_seconds`

Elapsed wall-clock time of the latest Phase 8 workflow invocation. It includes the workflow tasks and excludes the later metrics collection step.

## Files

### Added

```text
src/finbank/monitoring.py
src/finbank/operational_events.py

scripts/collect_metrics.py

sql/008_create_monitoring.sql
sql/009_monitoring_verification.sql

tests/test_monitoring.py

docs/monitoring-phase9.md
```

### Modified

```text
src/finbank/kafka_consumer.py
src/finbank/postgres.py
src/finbank/postgres_ingestion.py
src/finbank/quality_ingestion.py

scripts/init_database.py
scripts/run_pipeline.py
scripts/package_project.py

README.md
.gitignore
```

`dbt/models/**` is intentionally unchanged.

## New dependencies

None. Phase 9 uses only the Python standard library plus Psycopg already present from Phase 3.

The verified versions remain:

```text
dbt Core: 1.12.5
dbt-postgres: 1.11.0
```

## Verification

Use Windows CMD only.

### 1. Initialize the monitoring table

```cmd
python scripts\init_database.py
```

Expected:

```text
FinBank PostgreSQL schema initialized successfully.
Created/verified schemas: raw, staging, analytics, monitoring
```

### 2. Run the Python suite

```cmd
pytest -q
```

Expected: all Phase 9 unit/static tests pass.

### 3. Validate the package before packaging

```cmd
python scripts\validate_package.py
```

Expected:

```text
Package validation passed.
```

### 4. Run the Phase 8 pipeline once

```cmd
python scripts\run_pipeline.py
```

Expected: all workflow tasks complete successfully and `data\metrics\latest_pipeline_run.json` is written.

### 5. Collect metrics

```cmd
python scripts\collect_metrics.py
```

Expected output contains:

```text
FinBank metrics collected: processed=... rejected=... fraud_alerts=... consumer_errors=... database_errors=... output=...latest_metrics.json
```

### 6. Verify the metrics table

```cmd
psql -U finbank -h localhost -d finbank -f sql\009_monitoring_verification.sql
```

Expected:

- `metric_snapshot_count` is at least 1
- `invalid_metric_rows` is 0
- `invalid_pipeline_status_rows` is 0
- the recent snapshot displays the collected metrics

### 7. Collect a second snapshot

```cmd
python scripts\collect_metrics.py
```

Expected: the second invocation completes without error and adds another row to `monitoring.metric_snapshots`.

## Packaging safeguards

Before creating a ZIP:

```cmd
python scripts\validate_package.py
python scripts\package_project.py --output ..\finbank-data-platform-phase9.zip
```

The package script invokes validation first and excludes `__pycache__`, `.pytest_cache`, `.venv`, `target`, `dbt_packages`, and runtime `data/` directories (`generated`, `analytics`, and `metrics`).

## Deprecation warning note

The verified Phase 8/9 environment may still show dbt Core 1.12 generic-test argument deprecation warnings from existing schema YAML. That compatibility cleanup is intentionally deferred rather than mixed into the monitoring phase. No new dbt model behavior is introduced here.
