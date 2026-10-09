-- Phase 9 monitoring verification. Zero rows means the invariant checks passed.

SELECT
    COUNT(*) AS metric_snapshot_count
FROM monitoring.metric_snapshots;

SELECT
    COUNT(*) AS invalid_metric_rows
FROM monitoring.metric_snapshots
WHERE transactions_generated < 0
   OR transactions_processed < 0
   OR transactions_rejected < 0
   OR transactions_per_minute < 0
   OR fraud_alerts < 0
   OR high_risk_transactions < 0
   OR consumer_errors < 0
   OR database_errors < 0
   OR throughput_window_seconds < 0;

SELECT
    COUNT(*) AS invalid_pipeline_status_rows
FROM monitoring.metric_snapshots
WHERE pipeline_status NOT IN ('success', 'failed', 'unknown');

SELECT
    captured_at,
    transactions_generated,
    transactions_processed,
    transactions_rejected,
    transactions_per_minute,
    fraud_alerts,
    high_risk_transactions,
    consumer_errors,
    database_errors,
    pipeline_execution_time_seconds,
    pipeline_status
FROM monitoring.metric_snapshots
ORDER BY captured_at DESC
LIMIT 10;
