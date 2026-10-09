-- Phase 4 verification queries.

SELECT rejection_code, COUNT(*) AS rejected_count
FROM raw.rejected_records
GROUP BY rejection_code
ORDER BY rejection_code;

SELECT
    event_id,
    transaction_id,
    rejection_code,
    rejection_reason,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    rejected_at
FROM raw.rejected_records
ORDER BY rejected_at, rejection_id;

SELECT COUNT(*) AS valid_transaction_count
FROM raw.transaction_events;

SELECT COUNT(*) AS rejected_record_count
FROM raw.rejected_records;
