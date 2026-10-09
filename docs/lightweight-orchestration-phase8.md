# FinBank Phase 8 — Lightweight Orchestration

## Purpose

Phase 8 adds a small Python workflow runner instead of Apache Airflow.
This is the canonical orchestration path for the native Windows development environment.

No Airflow installation, scheduler service, Docker container, or Linux runtime is required.

## Environment

The verified development stack remains:

```text
Windows 10 Enterprise LTSC 2019
Kafka 3.9.2 — native Windows, KRaft
PostgreSQL 17.2 — native Windows
Python virtual environment
```

The Kafka and PostgreSQL services are not managed by this workflow runner. They remain local prerequisites.

## Workflow

```text
validate
   |
   v
dbt run
   |
   v
dbt test
   |
   v
generate analytics
```

Dependencies are represented explicitly in Python rather than encoded only by the order of a shell script.

## Tasks

### validate

Checks:

- required generated CSV files exist
- `dbt` is available on `PATH`
- PostgreSQL connection succeeds
- required `raw` relations exist

This is a read-only task.

### dbt_run

Runs:

```cmd
dbt run --project-dir dbt --profiles-dir "%USERPROFILE%\.dbt"
```

### dbt_test

Runs:

```cmd
dbt test --project-dir dbt --profiles-dir "%USERPROFILE%\.dbt"
```

Keeping `dbt run` and `dbt test` as separate tasks makes dependency and failure behavior visible.

### generate_analytics

Queries the analytics marts and writes:

```text
data/analytics/latest_report.json
```

The report includes completed transaction totals by currency, active customers, fraud alerts, high-risk alerts, suspicious transaction value, and rejected-record count.

Cross-currency values remain separated because FinBank has no FX conversion model.

## Retries

Each task supports bounded retries.

Default:

```text
initial attempt + 2 retries
```

The retry delay uses exponential backoff:

```text
2 seconds
4 seconds
```

A task is only considered successful when its action returns without raising an exception.

A failed task stops all downstream tasks.

## Idempotency

The workflow is designed so a rerun does not require a cleanup step:

- validation is read-only
- dbt mart tables are rebuildable using the existing idempotent constraint hooks
- dbt tests do not mutate data
- the analytics report is atomically replaced rather than appended

This is workflow idempotency: repeating the workflow does not create duplicate analytical state.

It is distinct from Kafka idempotency, which is handled by the PostgreSQL ingestion layer.

## Exit codes

```text
0 = all tasks completed
1 = workflow failure
```

The command can therefore be used by Windows Task Scheduler or another external scheduler.

## Manual execution

From the repository root:

```cmd
python scripts\run_pipeline.py
```

Dry-run the dependency graph without executing database/dbt work:

```cmd
python scripts\run_pipeline.py --dry-run
```

Increase retries explicitly:

```cmd
python scripts\run_pipeline.py --retries 3 --retry-delay-seconds 2
```

## Windows Task Scheduler

The runner is intentionally a normal command-line process.

A Task Scheduler action can invoke:

```text
Program/script:
  C:\path\to\finbank-data-platform\.venv\Scripts\python.exe

Arguments:
  C:\path\to\finbank-data-platform\scripts\run_pipeline.py

Start in:
  C:\path\to\finbank-data-platform
```

The process exit code is what the scheduler can use to identify success or failure.

## Why not Airflow?

Airflow is excluded from the core project because it is unnecessary for this local workload and conflicts with the project's Windows resource constraints and earlier machine-specific issues.

The workflow runner nevertheless demonstrates the concepts that matter:

- tasks
- dependencies
- retries
- logging
- failure propagation
- exit codes
- scheduling through an external scheduler
- idempotent reruns

A production deployment could replace the runner with Airflow, but that would be an orchestration-platform substitution rather than a redesign of the data transformations themselves.

## Why no extra scheduler service?

A resident scheduler would consume resources continuously and would add another operational component to a small local portfolio project.

Windows Task Scheduler is sufficient for scheduled execution while Python remains responsible for dependency ordering and task execution.

## Phase boundary

Phase 8 does not introduce operational metrics, Prometheus, Grafana, or dashboard infrastructure.

Those remain later concerns.
