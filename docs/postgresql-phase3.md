# Phase 3 — Native PostgreSQL on Windows

## Canonical database runtime

FinBank Phase 3 uses **native PostgreSQL on Windows**, not Docker, on the canonical development machine.

The target PostgreSQL major version is **17**.

PostgreSQL's official Windows download page links to the EDB-certified interactive installer. The current installer matrix lists PostgreSQL 17 on Windows 2019 among its tested 64-bit platforms. PostgreSQL's native Windows notes state that the native Windows port requires Windows 10 or later.

Official path:

- PostgreSQL downloads: https://www.postgresql.org/download/
- Windows installer page: https://www.postgresql.org/download/windows/

## 1. Install PostgreSQL

Use the Windows installer from the official PostgreSQL Windows download page.

Recommended local selections:

- PostgreSQL 17
- PostgreSQL Server
- Command Line Tools
- pgAdmin is optional but useful for inspecting the database
- Port: `5432`
- Keep the server bound to localhost for this local project
- Choose a strong local password for the `postgres` administrative account

Do not install additional extensions unless a later phase needs one.

## 2. Create the FinBank role and database

Open `psql` as the `postgres` administrator or use pgAdmin.

Create the application role:

```sql
CREATE ROLE finbank LOGIN PASSWORD 'change_me';
```

Create the database:

```sql
CREATE DATABASE finbank OWNER finbank;
```

For the local project, use a dedicated application role rather than running the Python consumer as the `postgres` superuser.

## 3. PostgreSQL configuration for an 8 GB developer machine

Locate the active `postgresql.conf` in the PostgreSQL data directory. The exact installation path depends on the installer selections.

Use a conservative local-development configuration such as:

```conf
listen_addresses = 'localhost'
port = 5432

shared_buffers = 128MB
work_mem = 4MB
maintenance_work_mem = 64MB
effective_cache_size = 1GB
max_connections = 20

wal_buffers = 4MB
checkpoint_timeout = 10min
max_wal_size = 512MB

fsync = on
synchronous_commit = on
```

### Why these values?

`shared_buffers` is intentionally conservative because the database is sharing an 8 GB machine with Windows, Kafka/JVM, Python, an editor, and potentially a browser. PostgreSQL documentation gives 25% of RAM as a starting point for a dedicated database server, but this is not a dedicated database host; the project therefore does not allocate that much memory to PostgreSQL.

`max_connections=20` prevents an accidentally large connection count from consuming unnecessary shared resources. PostgreSQL documentation notes that increasing `max_connections` increases resources allocated based on that setting.

`work_mem=4MB` and `maintenance_work_mem=64MB` are deliberately modest. `effective_cache_size=1GB` is a planner estimate, not a direct memory allocation.

`fsync=on` and `synchronous_commit=on` remain enabled. This project is demonstrating reliable ingestion, so local tuning must not disable durability in exchange for artificial benchmark numbers.

## 4. Keep PostgreSQL local-only

The PostgreSQL default `listen_addresses` is localhost. FinBank should retain that local-only posture:

```conf
listen_addresses = 'localhost'
```

Do not change this to `'*'` for the portfolio project.

The application's connection should therefore be:

```text
localhost:5432
```

## 5. Authentication

The installer-created `pg_hba.conf` should allow local password authentication. If explicit host rules are needed, use localhost only, for example:

```conf
host    finbank    finbank    127.0.0.1/32    scram-sha-256
host    finbank    finbank    ::1/128         scram-sha-256
```

Do not use `trust` merely to make local development easier. Password authentication makes the application configuration realistic and keeps the separation between administrative and application users.

Restart PostgreSQL after changing settings that require a server restart.

## 6. Configure FinBank

Create a local `.env` file from `.env.example` and set:

```text
POSTGRES_DSN=postgresql://finbank:change_me@localhost:5432/finbank
POSTGRES_CONNECT_TIMEOUT=5
POSTGRES_RETRY_ATTEMPTS=3
POSTGRES_RETRY_BACKOFF_SECONDS=1
```

The repository does not commit `.env`.

The Phase 3 Python code reads `POSTGRES_DSN` from the process environment. The `.env` file is a configuration template; the shell must expose those variables to the process. On Windows PowerShell, for example:

```powershell
$env:POSTGRES_DSN="postgresql://finbank:change_me@localhost:5432/finbank"
$env:POSTGRES_CONNECT_TIMEOUT="5"
$env:POSTGRES_RETRY_ATTEMPTS="3"
$env:POSTGRES_RETRY_BACKOFF_SECONDS="1"
```

If the password contains URL-special characters, percent-encode them in the DSN.

## 7. Install the Python PostgreSQL adapter

Phase 3 uses Psycopg 3:

```powershell
pip install -r requirements.txt
```

The pinned package is:

```text
psycopg[binary]==3.3.6
```

Psycopg supports Windows and current Python versions; the binary extra avoids requiring a local PostgreSQL client library build for the portfolio project.

## 8. Initialize FinBank schemas

Run:

```powershell
python scripts/init_database.py
```

This creates:

```text
raw
staging
analytics
```

and Phase 3 raw tables:

```text
raw.customers
raw.accounts
raw.merchants
raw.transaction_events
```

The staging and analytics schemas are created now because they are architectural boundaries for later dbt phases, but no dbt models are implemented in Phase 3.

## 9. Load reference data

Generate the deterministic source data if it does not already exist:

```powershell
python scripts/generate_data.py
```

Then load customers, accounts, and merchants:

```powershell
python scripts/load_reference_data.py
```

This loader is intentionally idempotent. Re-running it updates existing reference records rather than duplicating them.

## 10. Consume Kafka transactions into PostgreSQL

Kafka remains the **native Windows Kafka 3.9.2** broker at `localhost:9092`.

Do not change the FinBank documentation to Kafka 4.3.1: 4.3.1 was the optional Docker example from earlier material. The local verified broker for this project is 3.9.2. Kafka's standard client/server protocol is being used, but version-specific capabilities can differ.

Start the Phase 3 consumer:

```powershell
python scripts/consume_transactions_to_postgres.py --max-messages 10
```

Then publish ten events from another terminal:

```powershell
python scripts/produce_transactions.py --max-records 10
```

The consumer should log inserted transactions with their Kafka topic, partition, and offset.

## 11. Inspect the raw table

Use `psql` or pgAdmin:

```sql
SELECT
    event_id,
    transaction_id,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    ingested_at
FROM raw.transaction_events
ORDER BY kafka_partition, kafka_offset;
```

Check the count:

```sql
SELECT COUNT(*) FROM raw.transaction_events;
```

Expected after one ten-event demonstration:

```text
10
```

## 12. What the raw table preserves

`raw.transaction_events.payload` stores the complete JSON event received from Kafka.

Additional typed columns make the raw landing table operationally inspectable and let PostgreSQL enforce relationships to the reference entities:

```text
payload
customer_id
account_id
merchant_id
transaction_id
amount
currency
status
...
```

Kafka metadata is also stored:

```text
topic
partition
offset
```

and the ingestion timestamp is generated by PostgreSQL.

## 13. Idempotency and Kafka redelivery

The transaction event table has unique constraints on event identity and Kafka position. The insert uses:

```sql
ON CONFLICT DO NOTHING
```

The consumer ordering is:

```text
1. receive Kafka record
2. validate event envelope
3. begin PostgreSQL transaction
4. INSERT raw event
5. COMMIT PostgreSQL transaction
6. commit Kafka offset
```

If step 5 succeeds but step 6 fails, Kafka can redeliver the record. PostgreSQL sees the existing unique transaction identity and performs no second insert. The consumer then commits the Kafka offset on the successful replay.

### Controlled crash-window demonstration

The consumer has a development-only flag:

```powershell
python scripts/consume_transactions_to_postgres.py --max-messages 1 --fail-after-db-write
```

This deliberately raises an exception after the PostgreSQL write but before the Kafka offset commit.

Run the same consumer group again without the flag:

```powershell
python scripts/consume_transactions_to_postgres.py --max-messages 1
```

The second run should report the event as a duplicate rather than inserting another row.

That is the most important Phase 3 idempotency demonstration.

## 14. Duplicate event replay demonstration

Another useful test is to publish the same ten CSV rows again:

```powershell
python scripts/produce_transactions.py --max-records 10
```

Consume them with the same consumer group:

```powershell
python scripts/consume_transactions_to_postgres.py --max-messages 10
```

Then check:

```sql
SELECT COUNT(*) FROM raw.transaction_events;
```

The count should remain unchanged because `transaction_id` is unique.

## 15. Database failure and retry behavior

Phase 3 retries only transient connection failures (`OperationalError` / `InterfaceError`). Constraint violations are not retried because they represent a data or application problem rather than a temporary infrastructure outage.

The retry sequence is:

```text
attempt 1
   |
   X transient DB error
   |
  1s
   v
attempt 2
   |
   X transient DB error
   |
  2s
   v
attempt 3
```

The backoff is configurable through environment variables.


## Verified local environment

Phase 3 was verified end-to-end on the actual development environment:

```text
Windows 10 Enterprise LTSC 2019 (build 1809 / 17763)
Python virtual environment
Apache Kafka 3.9.2 — native Windows JVM process
PostgreSQL 17.2 — native Windows service
Docker — not used
```

The Kafka broker version is **3.9.2**, not the optional Docker example version `apache/kafka:4.3.1` mentioned in earlier material. The Python client communicates through Kafka's standard protocol; nevertheless, Kafka 3.9.2 and Kafka 4.3.1 are different broker versions, so newer Kafka features or defaults must not be assumed to exist in this verified environment.

## Kafka consumer close() failure mode

During the Phase 3 redelivery verification, FinBank exposed a subtle cleanup-path failure. `kafka-python`'s `KafkaConsumer.close()` defaults to behavior that can commit offsets during close. With manual offset management, calling `consumer.close()` without an explicit `autocommit=False` can therefore acknowledge a record that application code has not successfully completed.

The defensive pattern used by FinBank is:

```python
try:
    # consume/process/commit explicitly
    ...
finally:
    consumer.close(autocommit=False)
```

This is not merely stylistic. The Phase 3 test demonstrated the difference:

```text
Before fix:
PostgreSQL commit succeeded
        +
consumer.close() auto-committed offset
        -> redelivery could not be demonstrated

After fix:
PostgreSQL commit succeeded
        +
consumer.close(autocommit=False)
        -> Kafka offset remained uncommitted
        -> next run redelivered the event
        -> idempotent INSERT ignored the duplicate
```

**Known failure mode:** never allow a Kafka consumer cleanup call to silently acknowledge work when the application is deliberately controlling offsets.

**Defensive recommendation:** any future FinBank consumer using manual offset management should explicitly call `consumer.close(autocommit=False)` in its cleanup path.

## Test scope: unit tests versus live integration

`tests/test_postgres.py` does **not** use mocks and does **not** require a live PostgreSQL server. It is a unit-test module that checks:

- PostgreSQL configuration validation
- timestamp parsing
- the idempotent SQL statement structure
- required raw-ingestion columns

It therefore cannot prove that a PostgreSQL server accepted the SQL or that a Kafka record made it through the complete ingestion path.

The live integration path is verified separately with:

```powershell
python scripts/init_database.py
python scripts/load_reference_data.py
python scripts/consume_transactions_to_postgres.py --max-messages 10
```

plus direct SQL verification in `psql`/pgAdmin, duplicate replay, and the controlled redelivery test. This separation is intentional: unit tests stay fast and deterministic, while the infrastructure-dependent path is explicitly tested against the real services.

## 16. Why not Docker?

Docker is still useful for contributors on supported machines, so `docker/docker-compose.postgres.yml` provides an optional PostgreSQL 17 container.

It is not the canonical FinBank development path because the actual development machine cannot run Docker Desktop reliably. Infrastructure documentation should match the environment in which the project is actually tested.

## 17. Phase 3 limitations

This phase deliberately does **not** implement:

- invalid-record quarantine
- schema evolution tooling
- dead-letter topics
- connection pooling
- dbt
- analytical models
- fraud rules
- orchestration

Invalid business records will be introduced deliberately in Phase 4, where they will be routed into an inspectable quarantine table rather than silently disappearing.
