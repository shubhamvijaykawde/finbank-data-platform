-- Phase 5 dbt verification queries.
-- Run after: dbt build --project-dir dbt --profiles-dir "$HOME\.dbt"

-- Row counts by physical model.
SELECT 'staging.stg_customers' AS relation, COUNT(*) AS row_count FROM staging.stg_customers
UNION ALL
SELECT 'staging.stg_accounts', COUNT(*) FROM staging.stg_accounts
UNION ALL
SELECT 'staging.stg_merchants', COUNT(*) FROM staging.stg_merchants
UNION ALL
SELECT 'staging.stg_transactions', COUNT(*) FROM staging.stg_transactions
UNION ALL
SELECT 'staging.int_customer_transactions', COUNT(*) FROM staging.int_customer_transactions
UNION ALL
SELECT 'staging.int_daily_transactions', COUNT(*) FROM staging.int_daily_transactions
UNION ALL
SELECT 'staging.int_fraud_candidates', COUNT(*) FROM staging.int_fraud_candidates
UNION ALL
SELECT 'analytics.dim_customer', COUNT(*) FROM analytics.dim_customer
UNION ALL
SELECT 'analytics.dim_account', COUNT(*) FROM analytics.dim_account
UNION ALL
SELECT 'analytics.dim_merchant', COUNT(*) FROM analytics.dim_merchant
UNION ALL
SELECT 'analytics.dim_date', COUNT(*) FROM analytics.dim_date
UNION ALL
SELECT 'analytics.fact_transactions', COUNT(*) FROM analytics.fact_transactions
ORDER BY relation;

-- Fact-to-dimension relationship checks.
SELECT COUNT(*) AS orphan_customer_keys
FROM analytics.fact_transactions f
LEFT JOIN analytics.dim_customer d ON d.customer_sk = f.customer_sk
WHERE d.customer_sk IS NULL;

SELECT COUNT(*) AS orphan_account_keys
FROM analytics.fact_transactions f
LEFT JOIN analytics.dim_account d ON d.account_sk = f.account_sk
WHERE d.account_sk IS NULL;

SELECT COUNT(*) AS orphan_merchant_keys
FROM analytics.fact_transactions f
LEFT JOIN analytics.dim_merchant d ON d.merchant_sk = f.merchant_sk
WHERE d.merchant_sk IS NULL;

SELECT COUNT(*) AS orphan_date_keys
FROM analytics.fact_transactions f
LEFT JOIN analytics.dim_date d ON d.date_sk = f.date_sk
WHERE d.date_sk IS NULL;

-- Fact grain check: every transaction ID must occur once.
SELECT transaction_id, COUNT(*) AS row_count
FROM analytics.fact_transactions
GROUP BY transaction_id
HAVING COUNT(*) <> 1;
