# FinBank — End-to-End Data Engineering Platform

FinBank is a local portfolio project for building and testing a realistic banking data pipeline. I built it to work through the reliability problems behind event-driven data engineering—not just transform a CSV and plot a chart.

The platform generates synthetic banking data, streams transactions through Kafka, validates and lands events in PostgreSQL, models a dimensional warehouse with dbt, derives explainable fraud alerts, and presents business and pipeline-health metrics in a Streamlit dashboard.

> **Scope:** FinBank is a demonstration project, not a production banking or fraud-detection system. Its fraud rules and synthetic data are designed for engineering practice and reproducible tests, not for estimating real-world fraud prevalence.

## What it demonstrates

- **Event streaming:** Kafka in KRaft mode, keyed transaction events, manual consumer offset commits, and replay-aware ingestion.
- **Reliable ingestion:** PostgreSQL inserts are idempotent; duplicate events do not create duplicate transaction rows.
- **Data quality:** invalid events are rejected with reasons and retained in a quarantine table with their raw payload and Kafka provenance.
- **Analytics engineering:** dbt staging/intermediate models, dimensional marts, schema tests, and physical primary-key/unique constraints.
- **Explainable fraud analysis:** deterministic SQL rules, weighted risk scores, severity bands, and human-readable rule evidence.
- **Orchestration and observability:** a lightweight dependency-aware Python workflow, retries, persisted metric snapshots, and JSON operational state.
- **Business-focused dashboard:** banking, fraud, and operational views whose currency handling and metric caveats are explicit.
- **Testing:** Python unit/edge tests, gated live database/dbt integration tests, and a live end-to-end runner.

## Architecture

```mermaid
flowchart TD
    GEN["Deterministic Python data generator"] --> KAFKA["Kafka transaction topic"]
    KAFKA --> QC{"Quality-aware consumer"}
    QC -->|Valid| RAW[("PostgreSQL: raw.transaction_events")]
    QC -->|Invalid| REJ[("PostgreSQL: raw.rejected_records")]
    RAW --> DBT["dbt: staging → intermediate → marts"]
    DBT --> FACT[("analytics.fact_transactions")]
    FACT --> RULES["Explainable SQL fraud rules"]
    RULES --> ALERTS[("analytics.fact_fraud_alerts")]
    PIPE["Python workflow + metrics collector"] --> METRICS[("monitoring.metric_snapshots")]
    FACT --> DASH["Streamlit dashboard"]
    ALERTS --> DASH
    METRICS --> DASH
```

### Main data flow

1. The generator creates deterministic customer, account, merchant, and transaction data from a configurable seed.
2. A Python producer publishes transaction events to Kafka. The consumer validates reference IDs and transaction fields before storing events.
3. Valid events are written to `raw.transaction_events`; invalid events are recorded in `raw.rejected_records` with a rejection reason, raw payload, and Kafka topic/partition/offset.
4. dbt transforms the raw data into staging/intermediate relations and analytical marts, including `fact_transactions` and customer/account/merchant/date dimensions.
5. The fraud layer evaluates six explainable rules and produces at most one alert row per alerted transaction, aggregating rule evidence when multiple rules fire.
6. The workflow runner executes validation, dbt transformations/tests, and analytics generation. The monitoring layer records operational snapshots.
7. The Streamlit dashboard reads the warehouse and monitoring schemas to answer banking, fraud, and operational questions.

## Dashboard: business questions

| View | Questions answered | Key metrics |
|---|---|---|
| **Banking** | How much completed transaction activity is there, and how is volume changing over time? | Transaction value, count, average transaction, active customers, daily transaction count |
| **Fraud** | How many transactions were alerted, how severe are the alerts, and where is activity concentrated? | Fraud alerts, high-risk alerts, fraud rate, suspicious value, alerts by country and merchant category |
| **Operations** | How much data is accepted versus rejected, and are pipeline runs failing? | Processed/rejected transactions, processing rate, pipeline failures, latest run status |

**Currency is handled deliberately.** FinBank has no foreign-exchange normalization model, so EUR and GBP values are not added together. The dashboard can show a combined *count* view across currencies, but transaction-value totals are shown only for a selected currency.

**Fraud-rate caveat:** Fraud rate reflects demonstration data engineered to exercise the rule engine, not a calibrated production fraud rate. The end-to-end generator intentionally targets suspicious customers, and the warehouse can contain cumulative data from multiple development runs.

## Technology stack

| Layer | Technology | Role |
|---|---|---|
| Runtime | Python 3.14.8 | Data generation, producer/consumer, validation, orchestration, metrics, dashboard queries |
| Streaming | Apache Kafka 3.9.2, KRaft | Transaction event transport without ZooKeeper |
| Database | PostgreSQL 17.2 | Raw event storage, quarantine, analytics marts, and monitoring snapshots |
| Ingestion | `kafka-python`, `psycopg` 3 | Event production/consumption and database writes |
| Transformation | dbt Core 1.12.5, `dbt-postgres` 1.11.0 | Staging, intermediate, marts, and data tests |
| Fraud analytics | SQL rules and weighted scoring | Deterministic, evidence-backed alerts; no ML model |
| Orchestration | Python workflow runner | Dependency-aware execution, retries, failure propagation, exit codes |
| Monitoring | PostgreSQL + JSON snapshots | Pipeline/consumer/database operational metrics |
| Dashboard | Streamlit 1.65.0 | Local business and operations dashboard |
| Verification | pytest + dbt tests + live E2E script | Unit, edge, integration, and end-to-end coverage |

### Native Windows development

The verified development environment is **Windows 10 Enterprise LTSC 2019, build 1809**, with native Kafka and PostgreSQL. Docker Desktop/WSL2 are not part of the canonical local workflow. The repository retains optional Docker Compose files for environments that support them, but the verified path uses native services.

The local Kafka topology is intentionally small (single broker/controller, one partition, replication factor 1). It is suitable for development and demonstration—not high availability.

## Verification results

The latest native-machine verification recorded for this repository was:

| Check | Result |
|---|---|
| Python suite with strict pytest markers and both integration flags enabled | **94 passed, 0 skipped, 0 warnings** |
| dbt tests | **101 passed, 0 warnings, 0 errors** |
| Live end-to-end run against native Kafka + PostgreSQL + dbt | **Passed**: `generated=25 raw=25 fact=25 fraud_alerts=25` |
| dbt materialization regression | Two consecutive live `dbt build` runs passed |
| Dashboard | Streamlit rendered All currencies, EUR, and GBP views after the PostgreSQL NULL-currency fix |

The end-to-end run uses fresh transaction IDs and a unique Kafka topic. All 25 alerts in that demonstration run are consistent with the fixture being designed to exercise the fraud rules; this output must not be interpreted as a production fraud rate.

## Run locally (Windows CMD)

### Prerequisites

- Windows with Python 3.14 available.
- Apache Kafka 3.9.2 configured in KRaft mode and listening on `localhost:9092`.
- PostgreSQL 17 with a local `finbank` database and a role that can create/use the project schemas and tables.
- Java/JDK as required by the Kafka distribution.

For native Kafka setup, see [`docs/NATIVE_KAFKA_WINDOWS.md`](docs/NATIVE_KAFKA_WINDOWS.md). Database details are in [`docs/postgresql-phase3.md`](docs/postgresql-phase3.md).

### 1. Create an environment and install dependencies

From the repository root:

```cmd
python -m venv .venv
.venv\Scripts\activate.bat
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### 2. Configure PostgreSQL and dbt

Set the application and dbt connection variables in the same CMD window. Replace `YOUR_PASSWORD` with the local password you configured; do not commit credentials.

```cmd
set "POSTGRES_DSN=postgresql://finbank:YOUR_PASSWORD@localhost:5432/finbank"
set "KAFKA_BOOTSTRAP_SERVERS=localhost:9092"
set "DBT_POSTGRES_HOST=localhost"
set "DBT_POSTGRES_PORT=5432"
set "DBT_POSTGRES_USER=finbank"
set "DBT_POSTGRES_PASSWORD=YOUR_PASSWORD"
set "DBT_POSTGRES_DB=finbank"
```

Create the dbt profile directory and copy the example profile:

```cmd
if not exist "%USERPROFILE%\.dbt" mkdir "%USERPROFILE%\.dbt"
copy dbt\profiles.yml.example "%USERPROFILE%\.dbt\profiles.yml"
```

`.env.example` documents the supported environment variables, but the commands above set them explicitly for CMD. No local password is stored in the repository.

### 3. Start Kafka and run the live end-to-end check

Start the configured Kafka broker in a separate CMD window. Then return to the activated project environment and run:

```cmd
python scripts\run_end_to_end.py
```

The runner generates fresh data, initializes/verifies schemas, loads reference data, creates a unique Kafka topic, publishes and consumes the transactions, runs the pipeline/dbt tests, and checks that the run's transactions reached the raw and fact tables.

Expected success line for the default 25-transaction run:

```text
END-TO-END VERIFICATION PASSED: generated=25 raw=25 fact=25 fraud_alerts=25
```

This command requires live Kafka, PostgreSQL, and dbt. It is not a mocked test. It creates new demonstration data in the configured database; use a development database, not a production database.

### 4. Run the dashboard

```cmd
python scripts\collect_metrics.py
python scripts\run_dashboard.py
```

Open the local URL printed by Streamlit, normally `http://localhost:8501`. The dashboard reads existing analytics and monitoring tables in PostgreSQL.

## Tests

Run unit/static tests without requiring live PostgreSQL/dbt integration:

```cmd
pytest -q --strict-markers
```

To enable the native database and dbt integration tests, set both flags in the same CMD window:

```cmd
set FINBANK_RUN_DB_INTEGRATION_TESTS=1
set FINBANK_RUN_DBT_INTEGRATION_TESTS=1
pytest -q --strict-markers
```

The database test exercises real insertion, constraints, duplicate handling, and the dashboard's all-currencies query path. The dbt integration regression runs the build twice to protect idempotent physical constraints. See [`docs/testing-phase11.md`](docs/testing-phase11.md) for more detail.

You can also run dbt directly after configuring the profile:

```cmd
dbt build --project-dir dbt --profiles-dir "%USERPROFILE%\.dbt"
```

## Engineering decisions worth discussing

- **Manual offset commits + idempotent database writes:** Kafka offsets and database writes are coordinated defensively; replayed events do not create duplicate transaction rows.
- **Quarantine instead of silent drops:** invalid events retain their payload, provenance, and validation reason for investigation.
- **dbt tests plus physical constraints:** model contracts are tested in dbt, while critical grain/identity properties are enforced in PostgreSQL. The `pre_hook`/`post_hook` behavior is regression-tested across consecutive builds.
- **Explainable rules instead of unsupported ML claims:** six deterministic heuristics contribute weighted points to a score capped at 100. Alert evidence includes rule codes and readable reasons.
- **No false currency aggregation:** amounts remain separate across currencies in the absence of an FX conversion model.
- **Lightweight orchestration:** a small Python workflow was selected over adding an orchestration service solely for a local portfolio deployment.
- **Native infrastructure:** the project was kept runnable on the actual Windows environment and its resource limits, rather than depending on an unavailable container runtime.

## Fraud-rule overview

The SQL rule engine uses these demonstration heuristics:

| Rule | Example trigger | Weight |
|---|---|---:|
| Large transaction | Amount greater than 3,000 | +40 |
| Transaction burst | Prior customer transaction within 10 minutes | +20 |
| Country change | Country changes within 60 minutes | +35 |
| Unusual spending | Amount at least 5× the customer's prior average | +25 |
| Failed attempts before success | Prior failed attempts followed by a high-value completed transaction | +30 |
| Monitored merchant category | High-value transaction in selected electronics/travel/entertainment categories | +15 |

Weights are added and capped at 100. Severity bands are `HIGH` (70+), `MEDIUM` (40–69), and `LOW` (1–39). These thresholds are engineering/demo rules, not calibrated fraud probabilities or production controls.

## Repository layout

```text
finbank-data-platform/
├── dashboard/       # Streamlit dashboard entry point
├── dbt/             # Staging, intermediate, marts, schema/singular tests
├── docs/            # Phase design notes and native Windows setup
├── docker/          # Optional Compose alternatives; not the verified local path
├── scripts/         # CLI entry points, live E2E runner, pipeline, metrics
├── sql/             # Schema creation and database verification queries
├── src/finbank/     # Reusable Python implementation
├── tests/           # Unit, edge, and gated integration tests
├── .env.example     # Environment-variable reference; no secrets
├── pyproject.toml
├── requirements.txt
└── README.md
```

## Limitations and possible future extensions

- Synthetic transactions and heuristic alerting; no labelled fraud outcomes or calibrated fraud probabilities.
- No FX conversion, so transaction amounts are only meaningfully summed within a selected currency.
- Single-node local Kafka with replication factor 1; no high availability.
- No cloud deployment, distributed compute, lake/lakehouse, or production scheduler.

These are deliberate scope boundaries for a reproducible portfolio build. The project focuses on demonstrating reliable ingestion, warehouse modeling, quality controls, testability, and the ability to explain engineering trade-offs.
