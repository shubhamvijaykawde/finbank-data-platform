-- Phase 9 lightweight operational metrics storage.

CREATE SCHEMA IF NOT EXISTS monitoring;

CREATE TABLE IF NOT EXISTS monitoring.metric_snapshots (
    metric_snapshot_id BIGSERIAL PRIMARY KEY,
    captured_at TIMESTAMPTZ NOT NULL,
    transactions_generated BIGINT NOT NULL CHECK (transactions_generated >= 0),
    transactions_processed BIGINT NOT NULL CHECK (transactions_processed >= 0),
    transactions_rejected BIGINT NOT NULL CHECK (transactions_rejected >= 0),
    transactions_per_minute NUMERIC(18,4) NOT NULL CHECK (transactions_per_minute >= 0),
    fraud_alerts BIGINT NOT NULL CHECK (fraud_alerts >= 0),
    high_risk_transactions BIGINT NOT NULL CHECK (high_risk_transactions >= 0),
    consumer_errors BIGINT NOT NULL CHECK (consumer_errors >= 0),
    database_errors BIGINT NOT NULL CHECK (database_errors >= 0),
    pipeline_execution_time_seconds NUMERIC(18,4),
    pipeline_status TEXT NOT NULL CHECK (pipeline_status IN ('success', 'failed', 'unknown')),
    throughput_window_seconds NUMERIC(18,4) NOT NULL DEFAULT 0 CHECK (throughput_window_seconds >= 0),
    error_interval_start_utc TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_metric_snapshots_captured_at
    ON monitoring.metric_snapshots (captured_at DESC);
