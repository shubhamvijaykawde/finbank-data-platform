-- Phase 6 data-model verification. Run after `dbt build`.

-- Physical analytical relations and row counts.
SELECT 'analytics.dim_customer' AS relation, COUNT(*) AS row_count FROM analytics.dim_customer
UNION ALL
SELECT 'analytics.dim_account', COUNT(*) FROM analytics.dim_account
UNION ALL
SELECT 'analytics.dim_merchant', COUNT(*) FROM analytics.dim_merchant
UNION ALL
SELECT 'analytics.dim_date', COUNT(*) FROM analytics.dim_date
UNION ALL
SELECT 'analytics.fact_transactions', COUNT(*) FROM analytics.fact_transactions
ORDER BY relation;

-- Physical primary/unique constraints added to dbt-managed mart tables.
SELECT
    conrelid::regclass AS relation,
    conname AS constraint_name,
    contype AS constraint_type
FROM pg_constraint
WHERE connamespace = 'analytics'::regnamespace
  AND contype IN ('p', 'u')
ORDER BY conrelid::regclass::text, conname;

-- Logical foreign-key checks represented by dbt relationships tests.
-- Expected: zero rows for each query.
SELECT COUNT(*) AS orphan_account_customers
FROM analytics.dim_account a
LEFT JOIN analytics.dim_customer c
  ON c.customer_sk = a.customer_sk
WHERE c.customer_sk IS NULL;

SELECT COUNT(*) AS orphan_fact_customers
FROM analytics.fact_transactions f
LEFT JOIN analytics.dim_customer c
  ON c.customer_sk = f.customer_sk
WHERE c.customer_sk IS NULL;

SELECT COUNT(*) AS orphan_fact_accounts
FROM analytics.fact_transactions f
LEFT JOIN analytics.dim_account a
  ON a.account_sk = f.account_sk
WHERE a.account_sk IS NULL;

SELECT COUNT(*) AS orphan_fact_merchants
FROM analytics.fact_transactions f
LEFT JOIN analytics.dim_merchant m
  ON m.merchant_sk = f.merchant_sk
WHERE m.merchant_sk IS NULL;

SELECT COUNT(*) AS orphan_fact_dates
FROM analytics.fact_transactions f
LEFT JOIN analytics.dim_date d
  ON d.date_sk = f.date_sk
WHERE d.date_sk IS NULL;

-- Grain contract: one row per business transaction ID. Expected: zero rows.
SELECT transaction_id, COUNT(*) AS row_count
FROM analytics.fact_transactions
GROUP BY transaction_id
HAVING COUNT(*) <> 1;

-- Account/customer identity check. Expected: zero rows.
SELECT
    a.account_id,
    a.customer_id AS account_customer_id,
    c.customer_id AS dimension_customer_id
FROM analytics.dim_account a
JOIN analytics.dim_customer c
  ON c.customer_sk = a.customer_sk
WHERE a.customer_id <> c.customer_id;

-- Date continuity check. Expected: zero rows.
WITH ordered_dates AS (
    SELECT
        full_date,
        lead(full_date) OVER (ORDER BY full_date) AS next_date
    FROM analytics.dim_date
)
SELECT full_date, next_date
FROM ordered_dates
WHERE next_date IS NOT NULL
  AND next_date <> full_date + interval '1 day';
