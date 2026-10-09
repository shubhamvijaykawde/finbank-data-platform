# Native Kafka on Windows — FinBank Development Environment

## Why this path is canonical

The FinBank development machine is **Windows 10 Enterprise LTSC 2019, build 1809 (17763)**. Docker Desktop is not a viable local runtime on this machine because the required WSL2/Hyper-V paths are unavailable for this OS configuration.

FinBank therefore uses **native Apache Kafka 3.9.2 on Windows** as the canonical local Kafka broker.

This is an engineering decision: use the smallest infrastructure that the actual development environment can support reliably, while keeping the Kafka client/API contract unchanged for the application.

Apache Kafka 3.9 documentation explicitly supports starting Kafka from downloaded files in KRaft mode, without Docker. Kafka 3.9.2 is a maintenance release in the 3.9 line. See the official Apache documentation and release announcement.

## Versions used

- Operating system: Windows 10 Enterprise LTSC 2019, build 1809 (17763)
- Java: Temurin JDK 17
- Kafka: 3.9.2
- Installation directory: `C:\kafka`
- Broker listener: `localhost:9092`
- Controller listener: `localhost:9093`
- KRaft: enabled
- Partitions: 1
- Local retention: 24 hours
- JVM heap cap: 512 MB

## Installation

Download Apache Kafka 3.9.2 and extract it to:

```text
C:\kafka
```

Verify Java:

```powershell
java -version
```

Kafka 3.9 requires Java 8 or later; FinBank uses JDK 17 for the local environment.

## Local broker configuration

Edit:

```text
C:\kafka\config\kraft\server.properties
```

The local configuration used by FinBank includes:

```properties
log.dirs=C:/kafka/logs
log.retention.hours=24
num.partitions=1
advertised.listeners=PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093
node.id=1
controller.quorum.voters=1@localhost:9093
```

The broker is intentionally a single combined broker/controller node. Replication factor is therefore 1.

### JVM memory

Cap the broker JVM at 512 MB. In `bin\windows\kafka-server-start.bat`, set:

```text
KAFKA_HEAP_OPTS=-Xms256m -Xmx512m
```

The exact batch-file location/configuration can vary with the Kafka distribution; verify that the environment variable is applied before starting the broker.

## Format KRaft storage once

From `C:\kafka`:

```powershell
cd C:\kafka
```

Generate a cluster UUID:

```powershell
bin\windows\kafka-storage.bat random-uuid
```

Format the storage using the cluster UUID returned by the command:

```powershell
bin\windows\kafka-storage.bat format -t <CLUSTER_UUID> -c config\kraft\server.properties
```

**Do not repeat the format command against an existing local data directory unless you intentionally want to reset the Kafka cluster.** Formatting destroys the existing local Kafka log state.

## Start Kafka

```powershell
cd C:\kafka
bin\windows\kafka-server-start.bat config\kraft\server.properties
```

Keep this terminal running while using FinBank.

## Verify the broker

The FinBank client expects:

```text
localhost:9092
```

From the FinBank project:

```powershell
python scripts/create_kafka_topic.py
```

Expected topic:

```text
transactions
```

Verify topics directly if required:

```powershell
cd C:\kafka
bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --list
```

## FinBank-specific configuration

The project `.env.example` contains:

```text
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=transactions
KAFKA_CONSUMER_GROUP=finbank-transaction-consumer
```

No Python source change is required for the native broker because the FinBank clients connect through the standard Kafka protocol endpoint.

## Resource-conscious rationale

This topology uses:

- one broker
- one controller
- one partition
- replication factor 1
- 24-hour local retention
- 512 MB maximum JVM heap
- no ZooKeeper
- no Kafka Connect
- no Schema Registry
- no second broker

The purpose is to demonstrate Kafka concepts and reliable application behavior, not to emulate a production-sized Kafka cluster on an 8 GB laptop.

## Troubleshooting first-start coordinator warnings

A new Kafka consumer group may briefly produce coordinator-related retry warnings while Kafka initializes the internal consumer-offset infrastructure. FinBank treats transient connection/coordinator errors as operational startup behavior; retrying the client is preferable to changing the application architecture.

If the warnings do not clear, verify that the broker is fully started and that `localhost:9092` is reachable before debugging the Python application.

## Version note

The canonical local broker is **Kafka 3.9.2**, not the optional Docker image referenced by earlier Phase 2 material.

FinBank's Python Kafka client talks to the standard Kafka protocol, so the application did not need source changes when the broker moved from the documented Docker path to the native Windows path. Nevertheless, do not describe the local environment as Kafka 4.3.1: version-specific features and configuration behavior can differ.
