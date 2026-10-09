-- FinBank Phase 7 fraud verification queries.
-- Run after: dbt build --project-dir dbt --profiles-dir "%USERPROFILE%\.dbt"
--
-- These queries are intentionally read-only. They report verification results;
-- zero violation rows/counts indicate that the corresponding integrity check passes.

-- 1. Alert count.
SELECT
    'alert_count' AS check_name,
    COUNT(*) AS alert_count
FROM analytics.fact_fraud_alerts;

-- 2. Severity distribution.
SELECT
    severity,
    COUNT(*) AS alert_count
FROM analytics.fact_fraud_alerts
GROUP BY severity
ORDER BY
    CASE severity
        WHEN 'HIGH' THEN 1
        WHEN 'MEDIUM' THEN 2
        WHEN 'LOW' THEN 3
        ELSE 4
    END;

-- 3. Rule frequency.
SELECT
    TRIM(rule_name) AS rule_triggered,
    COUNT(*) AS alert_count
FROM analytics.fact_fraud_alerts
CROSS JOIN LATERAL regexp_split_to_table(
    rule_triggered,
    '[[:space:]]*;[[:space:]]*'
) AS rule_name
WHERE TRIM(rule_name) <> ''
GROUP BY TRIM(rule_name)
ORDER BY alert_count DESC, rule_triggered;

-- 4. Explanation integrity: every alert must have rule evidence and a reason.
SELECT
    'alerts_with_missing_rule_evidence' AS check_name,
    COUNT(*) AS violation_count
FROM analytics.fact_fraud_alerts
WHERE rule_triggered IS NULL
   OR BTRIM(rule_triggered) = ''
   OR reason IS NULL
   OR BTRIM(reason) = '';

-- 5. Duplicate alert IDs or duplicate business transaction IDs.
WITH duplicate_alert_ids AS (
    SELECT fraud_alert_id
    FROM analytics.fact_fraud_alerts
    GROUP BY fraud_alert_id
    HAVING COUNT(*) > 1
),
duplicate_transaction_ids AS (
    SELECT transaction_id
    FROM analytics.fact_fraud_alerts
    GROUP BY transaction_id
    HAVING COUNT(*) > 1
)
SELECT
    'duplicate_alert_ids' AS check_name,
    COUNT(*) AS violation_count
FROM duplicate_alert_ids
UNION ALL
SELECT
    'duplicate_transaction_ids' AS check_name,
    COUNT(*) AS violation_count
FROM duplicate_transaction_ids;

-- 6. Orphan alerts: every alert must resolve to a fact transaction and customer.
SELECT
    'orphan_fact_transactions' AS check_name,
    COUNT(*) AS violation_count
FROM analytics.fact_fraud_alerts a
LEFT JOIN analytics.fact_transactions f
    ON f.transaction_id = a.transaction_id
WHERE f.transaction_id IS NULL
UNION ALL
SELECT
    'orphan_customers' AS check_name,
    COUNT(*) AS violation_count
FROM analytics.fact_fraud_alerts a
LEFT JOIN analytics.dim_customer c
    ON c.customer_id = a.customer_id
WHERE c.customer_id IS NULL;

-- 7. Risk-score bounds and severity consistency.
SELECT
    'risk_score_out_of_bounds' AS check_name,
    COUNT(*) AS violation_count
FROM analytics.fact_fraud_alerts
WHERE risk_score < 0
   OR risk_score > 100
UNION ALL
SELECT
    'severity_score_mismatch' AS check_name,
    COUNT(*) AS violation_count
FROM analytics.fact_fraud_alerts
WHERE (severity = 'HIGH' AND risk_score < 70)
   OR (severity = 'MEDIUM' AND (risk_score < 40 OR risk_score >= 70))
   OR (severity = 'LOW' AND (risk_score < 1 OR risk_score >= 40));
