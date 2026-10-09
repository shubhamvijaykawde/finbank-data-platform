# Phase 4 — Data Quality and Quarantine

## Objective

Phase 4 introduces controlled invalid transaction events and ensures that bad data is explicitly rejected, preserved, and inspectable rather than silently disappearing.

The pipeline becomes:

```text
Kafka 3.9.2 (native Windows)
        |
        v
Quality-aware Python consumer
        |
        +-------------------+
        |                   |
      valid               invalid
        |                   |
        v                   v
raw.transaction_events   raw.rejected_records
        |                   |
        +---------+---------+
                  |
                  v
             commit Kafka offset
```

A record is acknowledged by Kafka only after its corresponding PostgreSQL transaction succeeds.

## 1. Invalid records introduced in this phase

The controlled producer can create:

```text
missing_customer_id
missing_account_id
negative_amount
invalid_currency
unknown_merchant
duplicate_transaction
future_timestamp
invalid_status
```

Run:

```powershell
python scripts/produce_invalid_transactions.py
```

By default it publishes one event for each of the eight cases. The source transaction is taken from the deterministic Phase 1 dataset, so the corruption itself is deliberate and reproducible.

The duplicate case uses a distinct `event_id` but reuses the existing `transaction_id`. That distinguishes a business-level duplicate from Kafka redelivery of the same event. The normal Phase 2 producer still uses the transaction ID as both identities.

## 2. Validation layers

Validation occurs in two stages.

### Event/field validation

The Python validator checks:

- required transaction fields
- customer/account presence
- positive amount
- supported currency (`EUR`, `GBP`)
- timezone-aware ISO timestamp
- future timestamp
- allowed transaction status

### Database-backed validation

The PostgreSQL validator checks:

- customer exists
- account exists
- account belongs to the specified customer
- merchant exists
- transaction ID has not already been ingested

The database checks are necessary because the application cannot know from the Kafka payload alone whether an ID exists in the current reference data.

## 3. Quarantine table

Phase 4 adds:

```text
raw.rejected_records
```

Each Kafka record produces at most one quarantine row, even when multiple validation rules fail. Multiple reasons are combined into the same row so Kafka position remains the unique ingestion identity.

Stored information includes:

```text
rejection_id
event_id
transaction_id
rejection_code
rejection_reason
raw_payload
kafka_topic
kafka_partition
kafka_offset
rejected_at
```

The raw payload is stored as text so even malformed JSON can be preserved.

The table is idempotent on `(kafka_topic, kafka_partition, kafka_offset)`.

## 4. Processing semantics

### Valid record

```text
Kafka record
    |
    v
Field validation
    |
    v
Reference-data validation
    |
    v
raw.transaction_events
    |
    v
PostgreSQL commit
    |
    v
Kafka offset commit
```

### Invalid record

```text
Kafka record
    |
    v
Validation failure
    |
    v
raw.rejected_records
    |
    v
PostgreSQL commit
    |
    v
Kafka offset commit
```

### Infrastructure failure

If PostgreSQL cannot commit the valid/rejected outcome, the Kafka offset is not committed. The record can therefore be redelivered.

## 5. Why duplicates are handled differently

There are two distinct cases:

### Kafka redelivery

The same event is processed again after a previous database commit. The Phase 3 raw sink remains idempotent and reports a duplicate without creating another transaction row.

### Business duplicate

A different event carries a transaction ID that already exists. Phase 4 treats this as a data-quality problem and places it in `raw.rejected_records`.

This distinction prevents the quarantine layer from breaking the Phase 3 at-least-once + idempotent-sink design.

## 6. Verification

Initialize or upgrade the database:

```powershell
python scripts/init_database.py
```

Start the quality-aware consumer with a fresh group. The default `latest` offset policy is deliberate: it avoids replaying the older valid Phase 2/3 events already present in the topic.

```powershell
python scripts/consume_transactions_with_quality.py --group-id finbank-quality-demo --max-messages 8
```

Keep that terminal running, then publish the eight invalid events from another terminal:

```powershell
python scripts/produce_invalid_transactions.py
```

Then inspect:

```sql
SELECT rejection_code, COUNT(*) AS rejected_count
FROM raw.rejected_records
GROUP BY rejection_code
ORDER BY rejection_code;

SELECT COUNT(*) FROM raw.transaction_events;
SELECT COUNT(*) FROM raw.rejected_records;
```

Use `sql/004_phase4_verification.sql` for the complete query set.

Do not claim successful quarantine counts until the live Kafka + PostgreSQL run has been executed.

## 7. Phase 4 limitations

This phase does not implement:

- a Kafka dead-letter topic
- schema registry
- automated correction of bad records
- ML-based data-quality scoring
- distributed validation infrastructure

The quarantine table is intentionally simple and local so it remains appropriate for FinBank's development hardware.

## 8. Verified behavior and known fixture dependency

Phase 4 was verified end-to-end on the canonical development environment:

- Windows 10 Enterprise LTSC 2019, build 1809 (17763)
- Kafka 3.9.2, native Windows, KRaft mode
- PostgreSQL 17.2, native Windows
- Docker not used in the development path

The verified run published eight controlled invalid events and processed them as:

```text
processed=8 inserted=0 duplicates=0 rejected=8
```

All quarantine rows preserved the Kafka topic, partition, offset, and original raw payload.

### Duplicate fixture state

The current invalid-event fixture intentionally reuses `TXN00000001` for the cases. The
`duplicate_transaction` case therefore depends on a pre-existing row with
`transaction_id = TXN00000001` in `raw.transaction_events`. On an empty raw transaction
table, that particular event is not a duplicate and may be inserted as a valid row.

Therefore the exact eight-rejection demonstration is **state-dependent**. For a clean-room
run, first seed/ingest the known `TXN00000001` transaction, then publish the invalid fixture.
A future fixture improvement can make this completely self-contained by using distinct
transaction IDs and explicitly seeding one known duplicate target.

### Multiple rejection reasons

A single Kafka record is represented by at most one quarantine row. When the same event
violates multiple rules, FinBank aggregates all detected reason codes into one
`MULTIPLE` rejection and preserves the individual human-readable reasons in
`rejection_reason`, separated by `; `.

For example, an event can simultaneously have `UNKNOWN_MERCHANT` and
`DUPLICATE_TRANSACTION_ID`, producing:

```text
rejection_code   = MULTIPLE
rejection_reason = merchant_id 'MER999999' ...; transaction_id 'TXN00000001' ...
```

This is intentional: the Kafka position `(topic, partition, offset)` remains the unique
ingestion identity, while no detected quality failure is discarded.

### Serializer deprecation warning

`kafka-python` 3.x changed its preferred Serializer/Deserializer interface to include
`headers`. Passing plain callables still works through a compatibility wrapper but emits
a `DeprecationWarning`. FinBank now uses `DefaultSerializer` for the Kafka message key
and a `Serializer` implementation for the JSON event value in the shared producer factory.
Both the normal and invalid-event producers therefore use the same serializer pattern.

When developing future Kafka consumers/producers with manual offset management, also
retain the defensive `consumer.close(autocommit=False)` pattern documented in Phase 3.
