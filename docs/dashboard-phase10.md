# FinBank Phase 10 — Business Dashboard

## Technology

FinBank uses **Streamlit 1.65.0** as its single lightweight dashboard technology. Streamlit 1.65.0 is pinned for reproducibility and supports Python 3.14.

The dashboard is local and connects directly to the existing PostgreSQL analytical/operational schemas. No Docker, WSL2, Airflow, Grafana, Prometheus, Elasticsearch, or Node.js service is introduced.

## Run

From Windows CMD:

```cmd
pip install -r requirements.txt
python scripts\run_dashboard.py
```

Open the local Streamlit URL shown in the terminal, normally `http://localhost:8501`.

The dashboard expects `POSTGRES_DSN` to be configured as described in `.env.example`.

## Business questions

### Banking

**Question:** How much completed transaction activity are we processing, and how is daily payment volume changing?

Shows:

- total transaction value for the selected currency
- transaction count
- average transaction for the selected currency
- active customers
- average completed transactions per observed day
- daily completed transaction volume

EUR and GBP are intentionally not summed together. The dashboard makes currency selection explicit because the project has no FX normalization model.

### Fraud

**Question:** Where is suspicious activity concentrated, how severe is it, and what share of completed transactions are being alerted?

Shows:

- fraud alerts
- high-risk alerts
- fraud rate
- suspicious transaction value for the selected currency
- fraud by country
- fraud by merchant category

The fraud rate is the number of alerted transactions divided by valid analytical transactions in the selected currency, or across all currencies when no currency is selected.

> **Interpretation caveat:** Fraud rate reflects demonstration data engineered to exercise the rule engine, not a calibrated production fraud rate. The end-to-end generator intentionally targets suspicious customers, and the warehouse includes cumulative demonstration runs from multiple project phases.

### Operations

**Question:** Is the ingestion pipeline healthy, how much data is accepted versus rejected, and are pipeline runs failing?

Shows:

- processed transactions
- rejected transactions
- processing rate
- pipeline failures
- latest pipeline status

Pipeline failures are counted from persisted `monitoring.metric_snapshots` rows whose `pipeline_status` is `failed`.

## Data sources

- `analytics.fact_transactions`
- `analytics.fact_fraud_alerts`
- `raw.transaction_events`
- `raw.rejected_records`
- `monitoring.metric_snapshots`

## Verification

Unit tests cover dashboard query mapping and business-metric transformations. A live dashboard check requires the native PostgreSQL environment because the app intentionally reads real analytical tables.
