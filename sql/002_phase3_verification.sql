-- Phase 3 verification queries.

SELECT COUNT(*) AS customers FROM raw.customers;
SELECT COUNT(*) AS accounts FROM raw.accounts;
SELECT COUNT(*) AS merchants FROM raw.merchants;
SELECT COUNT(*) AS transaction_events FROM raw.transaction_events;

SELECT
    event_id,
    transaction_id,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    ingested_at
FROM raw.transaction_events
ORDER BY kafka_partition, kafka_offset
LIMIT 20;

SELECT
    transaction_id,
    customer_id,
    account_id,
    merchant_id,
    amount,
    currency,
    status
FROM raw.transaction_events
ORDER BY ingested_at DESC
LIMIT 20;
