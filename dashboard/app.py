"""Interactive Streamlit dashboard for FinBank business and operations questions."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
import sys

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPOSITORY_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

import streamlit as st

from finbank.dashboard import (
    BankingSummary,
    load_active_customers,
    load_banking_summary,
    load_fraud_by_category,
    load_fraud_by_country,
    load_fraud_summary,
    load_operations_summary,
    load_transactions_per_day,
)
from finbank.postgres_config import PostgresConfig


st.set_page_config(
    page_title="FinBank Data Platform",
    page_icon="🏦",
    layout="wide",
)

st.title("FinBank Data Engineering Dashboard")
st.caption("Business-facing view of analytical marts and pipeline health")

try:
    config = PostgresConfig.from_environment()
except ValueError as exc:
    st.error(str(exc))
    st.stop()


@st.cache_data(ttl=30)
def get_banking_summary() -> list[BankingSummary]:
    return load_banking_summary(config)


@st.cache_data(ttl=30)
def get_active_customers() -> int:
    return load_active_customers(config)


@st.cache_data(ttl=30)
def get_transactions_per_day(currency: str | None) -> list[dict[str, object]]:
    return load_transactions_per_day(config, currency)


@st.cache_data(ttl=30)
def get_fraud_summary(currency: str | None) -> dict[str, object]:
    return load_fraud_summary(config, currency)


@st.cache_data(ttl=30)
def get_fraud_by_country(currency: str | None) -> list[dict[str, object]]:
    return load_fraud_by_country(config, currency)


@st.cache_data(ttl=30)
def get_fraud_by_category(currency: str | None) -> list[dict[str, object]]:
    return load_fraud_by_category(config, currency)


@st.cache_data(ttl=30)
def get_operations_summary():
    return load_operations_summary(config)


try:
    banking = get_banking_summary()
    currencies = [row.currency for row in banking]
    with st.sidebar:
        st.header("Analysis filters")
        currency_label = st.selectbox(
            "Completed-transaction currency",
            ["All currencies", *currencies],
            help="EUR and GBP amounts are not summed together because no FX normalization exists in FinBank.",
        )
    selected_currency = None if currency_label == "All currencies" else currency_label
except Exception as exc:  # noqa: BLE001 - dashboard boundary
    st.error(f"Database read failed: {exc}")
    st.stop()

selected_summary = next(
    (row for row in banking if row.currency == selected_currency),
    None,
) if selected_currency else None

st.header("Banking")
st.markdown("**Business question:** How much completed transaction activity are we processing, and how is daily customer payment volume changing?")
if selected_summary:
    value_label = f"{selected_summary.currency} completed value"
    value_text = f"{selected_summary.transaction_value:,.2f}"
    avg_text = f"{selected_summary.average_transaction:,.2f}"
    active_customers = selected_summary.active_customers
    transaction_count = selected_summary.transaction_count
else:
    value_label = "Completed value (select currency)"
    value_text = "—"
    avg_text = "—"
    active_customers = get_active_customers()
    transaction_count = sum(row.transaction_count for row in banking) if banking else 0

bank_cols = st.columns(5)
bank_cols[0].metric("Total transaction value", value_text, help=value_label)
bank_cols[1].metric("Transaction count", f"{transaction_count:,}")
bank_cols[2].metric("Average transaction", avg_text, help="Amount is shown for the selected currency.")
bank_cols[3].metric("Active customers", f"{active_customers:,}")

transactions_per_day = get_transactions_per_day(selected_currency)
if transactions_per_day:
    bank_cols[4].metric("Transactions per day", f"{sum(row['transaction_count'] for row in transactions_per_day) / len(transactions_per_day):,.1f}", help="Average completed transaction count per observed calendar day.")
    st.subheader("Daily transaction volume")
    st.line_chart(
        {row["transaction_day"]: row["transaction_count"] for row in transactions_per_day},
        y_label="Completed transactions",
    )
else:
    bank_cols[4].metric("Transactions per day", "—")
    st.info("No completed transactions match the selected currency.")

st.header("Fraud")
st.markdown("**Business question:** Where is suspicious activity concentrated, how severe is it, and what fraction of completed transactions are being alerted?")
fraud = get_fraud_summary(selected_currency)
fraud_cols = st.columns(4)
fraud_cols[0].metric("Fraud alerts", f"{fraud['fraud_alerts']:,}")
fraud_cols[1].metric("High-risk alerts", f"{fraud['high_risk_alerts']:,}")
fraud_cols[2].metric("Fraud rate", f"{float(fraud['fraud_rate']) * 100:.2f}%")
fraud_cols[3].metric(
    "Suspicious transaction value",
    f"{Decimal(fraud['suspicious_transaction_value']):,.2f}",
    help="Alerted amount in the selected currency; select a currency to make the amount comparable.",
)

fraud_country = get_fraud_by_country(selected_currency)
fraud_category = get_fraud_by_category(selected_currency)
chart_cols = st.columns(2)
with chart_cols[0]:
    st.subheader("Fraud by country")
    if fraud_country:
        st.bar_chart({row["country"]: row["fraud_alerts"] for row in fraud_country}, y_label="Fraud alerts")
    else:
        st.info("No fraud alerts for the selected filter.")
with chart_cols[1]:
    st.subheader("Fraud by merchant category")
    if fraud_category:
        st.bar_chart({row["merchant_category"]: row["fraud_alerts"] for row in fraud_category}, y_label="Fraud alerts")
    else:
        st.info("No fraud alerts for the selected filter.")

st.header("Operations")
st.markdown("**Business question:** Is the ingestion pipeline healthy, how much data is being accepted or rejected, and are pipeline runs failing?" )
ops = get_operations_summary()
ops_cols = st.columns(4)
ops_cols[0].metric("Processed transactions", f"{ops.processed_transactions:,}")
ops_cols[1].metric("Rejected transactions", f"{ops.rejected_transactions:,}")
ops_cols[2].metric("Processing rate", f"{ops.processing_rate * 100:.2f}%")
ops_cols[3].metric("Pipeline failures", f"{ops.pipeline_failures:,}")
status = ops.latest_pipeline_status.lower()
if status == "success":
    st.success("Latest pipeline status: success")
elif status == "failed":
    st.error("Latest pipeline status: failed")
else:
    st.warning(f"Latest pipeline status: {ops.latest_pipeline_status}")

st.divider()
st.caption(
    "Data source: analytics.fact_transactions, analytics.fact_fraud_alerts, "
    "raw.transaction_events, raw.rejected_records, monitoring.metric_snapshots. "
    "Amounts are never summed across EUR and GBP without FX normalization."
)
