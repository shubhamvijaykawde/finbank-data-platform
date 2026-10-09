# FinBank Phase 2 — Kafka

## Scope

Phase 2 introduced one Kafka topic, one Python producer, and one Python consumer.

PostgreSQL and downstream processing are introduced only in Phase 3.

## Verified development environment

The canonical FinBank development machine is Windows 10 Enterprise LTSC 2019, build 1809 (17763). Docker Desktop is not available on this environment, so the verified Kafka broker is:

```text
Apache Kafka 3.9.2
native Windows installation
KRaft mode
localhost:9092
```

This is not the Docker `apache/kafka:4.3.1` environment mentioned in earlier material. The Python source did not change because it communicates using the standard Kafka client protocol, but version-specific Kafka capabilities must be considered against 3.9.2.

The exact native setup is documented in [`NATIVE_KAFKA_WINDOWS.md`](NATIVE_KAFKA_WINDOWS.md).

## Architecture

```text
Generated transactions.csv
          |
          v
  Python Kafka Producer
          |
          v
       Kafka 3.9.2
   transactions / P0
          |
          v
  Python Kafka Consumer
          |
          v
  Event-envelope validation
```

## Technology decision

FinBank uses `kafka-python` 3.0.11 for the Python client. The project intentionally avoids a larger client stack because producer, consumer, and topic administration are sufficient for the local portfolio project.

Kafka runs as one combined broker/controller node in KRaft mode. One broker and one partition minimize RAM and CPU usage while still exposing the core Kafka concepts needed by the project.

## Kafka concepts in FinBank

### Producer

`produce_transactions.py` reads the generated transaction CSV and publishes one Kafka event per transaction.

### Topic

`transactions` is the event stream for banking transaction events.

### Partition

Phase 2 uses exactly one partition. A partition is an ordered append-only sequence of records.

### Offset

Each record in a partition receives an offset. The consumer logs the topic, partition, and offset.

### Consumer

`consume_transactions.py` reads transaction events from the `transactions` topic.

### Consumer group

The default group is `finbank-transaction-consumer`. Kafka tracks committed offsets per consumer group, allowing the group to resume from its last committed position.

### Message key

The producer uses `transaction_id` as the Kafka message key. With multiple partitions, Kafka can use the key to consistently route related records to a partition. Phase 2 has one partition, so the immediate value is identity and observability.

### Serialization

Events use UTF-8 JSON. The envelope contains:

```json
{
  "event_id": "TXN00000001",
  "event_type": "transaction.created",
  "schema_version": 1,
  "event_timestamp": "2025-12-30T12:30:00+00:00",
  "transaction": { "...": "..." }
}
```

## Producer delivery semantics

The producer uses `acks=all`, bounded retries, and waits for each send future to complete.

## Consumer offset semantics

The consumer uses:

- `enable_auto_commit=False`
- `auto_offset_reset=earliest`

The offset is committed only after the event envelope is successfully deserialized and validated.

This gives the Phase 2 at-least-once processing shape that Phase 3 now connects to PostgreSQL:

```text
poll message
   |
   v
validate event
   |
   +---- failure ---> no commit ---> record can be redelivered
   |
   v
process
   |
   v
commit offset
```

Phase 3 changes `process` from logging only to an idempotent PostgreSQL transaction.

## Verified Phase 2 results

The local native Kafka verification completed successfully:

- `20` Python unit tests passed in the combined project test suite.
- Topic `transactions` was created with 1 partition and replication factor 1.
- Ten produced events were consumed with offsets 0 through 9.
- A second consumer group replayed the same ten records from the beginning.
- Message keys matched `transaction_id`.
- Manual Kafka offset commits were enabled.

These are development-environment verification results, not production benchmark claims.

## Startup coordinator warnings

On the first use of a brand-new consumer group, Kafka may briefly emit coordinator retry warnings while the internal consumer-offset infrastructure initializes. In the verified Windows setup this behavior cleared automatically and the consumer joined successfully.

The correct response is to allow the broker to finish startup and retry, not to change the project architecture.

## Why one partition?

The hardware constraint matters more than simulating a cluster. One partition avoids unnecessary parallelism while still allowing the project to demonstrate offsets and consumer groups.

## Docker alternative

`docker/docker-compose.yml` remains only as an optional Docker runtime for contributors whose machines support Docker. It is not the canonical FinBank development environment.


## Event identity

FinBank distinguishes event identity from business transaction identity:

- `event_id` identifies the published event.
- `transaction.transaction_id` identifies the banking transaction.
- Normal generated events use the same ID for both.
- A later data-quality test can publish a distinct event ID carrying an existing transaction ID to represent a business duplicate.

## Manual offset cleanup

All FinBank consumers use manual offset management and defensively close with:

```python
consumer.close(autocommit=False)
```

This prevents cleanup from acknowledging offsets that application code did not explicitly commit. The failure mode and Phase 3 reproduction are documented in `docs/postgresql-phase3.md`.
