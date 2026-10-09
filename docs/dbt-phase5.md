# Phase 5 — dbt ELT transformations on native Windows

## Objective

Phase 5 introduces dbt against the existing native PostgreSQL 17.2 database. The goal is
to build a clean transformation layer from the `raw` ingestion schema into staging,
intermediate, and analytical mart models.

The local execution path is explicitly:

```text
Windows 10 Enterprise LTSC 2019
        |
        +-- native Kafka 3.9.2
        |
        +-- native PostgreSQL 17.2
        |
        +-- Python virtual environment
                |
                +-- dbt-postgres 1.11.0
```

No Docker, WSL2, Linux VM, Spark, or Airflow is required for Phase 5.

## Version decision

The Phase 5 project accepts dbt versions `>=1.11.0,<1.13.0`. The development environment
was verified with dbt 1.12.5 and the repository retains `dbt-postgres==1.11.0` as the adapter
dependency. No Docker or Linux runtime is required.

Official references:

- dbt PostgreSQL adapter: https://docs.getdbt.com/docs/local/connect-data-platform/postgres-setup
- dbt profiles: https://docs.getdbt.com/docs/local/profiles.yml
- dbt staging best practices: https://docs.getdbt.com/best-practices/how-we-structure/2-staging
- dbt materializations: https://docs.getdbt.com/docs/build/materializations

## Verified Phase 5 result

The live Phase 5 verification was completed on the canonical native-Windows environment:

```text
Kafka 3.9.2      native Windows
PostgreSQL 17.2  native Windows
dbt Core 1.12.5
Docker           not used
```

The complete `dbt build` result was:

```text
PASS=88 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=88
```

The verification queries confirmed all twelve Phase 5 relations existed with the expected
model-level row counts, four fact/dimension orphan checks returned zero, and the duplicate
`transaction_id` check returned zero rows.

## Why dbt here?

Phase 3/4 produce source-conformed data. dbt supplies the transformation layer that moves
that source-conformed data toward business-conformed analytical models. dbt models are SQL
select statements that can be tested, documented, referenced as dependencies, and rebuilt
consistently.

## Layer design

### Staging

Models:

```text
stg_customers
stg_accounts
stg_merchants
stg_transactions
```

Staging keeps the original row grain and performs lightweight cleaning such as trimming
text, explicit numeric casts, and consistent column naming. It does not aggregate rows.
This follows dbt's staging guidance to keep atomic source concepts intact.

Physical schema: `staging`
Materialization: `view`

### Intermediate

Models:

```text
int_customer_transactions
int_daily_transactions
int_fraud_candidates
```

`int_customer_transactions` enriches each transaction with customer, account, and merchant
context and computes historical behavioral features using window functions.

`int_daily_transactions` changes the grain intentionally to daily aggregates.

`int_fraud_candidates` is **not a fraud detector**. It is the feature-ready input for the
future Phase 7 rule engine. It contains interpretable behavioral inputs but does not assign
risk scores, severity, or alerts.

Physical schema: `staging`
Materialization: `view`

### Marts

Models:

```text
dim_customer
dim_account
dim_merchant
dim_date
fact_transactions
```

Physical schema: `analytics`
Materialization: `table`

A mart table is intentionally persisted because these models are direct BI/analytics
consumption objects. dbt guidance generally recommends views for light staging logic and
tables for models that are queried by BI tools.

`fact_fraud_alerts` is intentionally deferred until Phase 7, when a real alert-producing
logic and alert grain exist. Creating it now would be a misleading placeholder.

## Data model and grain

### `dim_customer`

Grain: one row per customer.

Natural key: `customer_id`
Surrogate key: `customer_sk`

### `dim_account`

Grain: one row per account.

Natural key: `account_id`
Surrogate key: `account_sk`

### `dim_merchant`

Grain: one row per merchant.

Natural key: `merchant_id`
Surrogate key: `merchant_sk`

### `dim_date`

Grain: one row per calendar date in the transaction date range.

Surrogate key: integer `YYYYMMDD` stored as `date_sk`.

### `fact_transactions`

Grain: one row per valid transaction retained in `raw.transaction_events`.

Measures include transaction `amount`; descriptive foreign keys point to the customer,
account, merchant, and date dimensions. Kafka provenance remains available through topic,
partition, and offset columns.

## Surrogate keys

FinBank uses deterministic PostgreSQL `md5()` expressions such as:

```sql
md5('customer:' || customer_id)
```

The namespace prefix prevents the same natural ID format across different entities from
producing the same surrogate key expression. No external package is needed.

These are stable warehouse keys, not claims that they provide cryptographic security.

## Materialization decision

Phase 5 does **not** use incremental models. The local dataset is only 10,000 transactions,
and staging/intermediate transformations are lightweight. Incremental materialization adds
state and merge complexity, so it is not justified until actual build timings show a problem.

This is consistent with dbt's guidance that incremental models are most useful when table
rebuild time becomes unacceptable; they also require an explicit incremental filter and
unique key.

The first optimization is therefore to establish correct models and measure them.

## Local Windows setup

### 1. Activate the existing virtual environment

```powershell
.venv\Scripts\Activate.ps1
```

### 2. Install dbt

```powershell
python -m pip install -r requirements.txt
```

Verify:

```powershell
dbt --version
```

Expected adapter family:

```text
dbt-postgres 1.11.0
```

The exact command output can include additional dependency/version information; do not
claim exact local output until it has been run on the development machine.

### 3. Create the local dbt profile

Make the user-level directory if needed:

```powershell
New-Item -ItemType Directory -Force "$HOME\.dbt"
```

Copy the example:

```powershell
Copy-Item dbt\profiles.yml.example "$HOME\.dbt\profiles.yml"
```

Set the connection variables in the current PowerShell session:

```powershell
$env:DBT_POSTGRES_HOST="localhost"
$env:DBT_POSTGRES_PORT="5432"
$env:DBT_POSTGRES_DB="finbank"
$env:DBT_POSTGRES_USER="finbank"
$env:DBT_POSTGRES_PASSWORD="<your-finbank-password>"
```

This is intentionally separate from the Python `POSTGRES_DSN`. dbt's PostgreSQL adapter
expects individual connection fields in `profiles.yml`; the Python application can continue
to use the DSN.

### 4. Validate connectivity and project configuration

From the repository root:

```powershell
dbt debug --project-dir dbt --profiles-dir "$HOME\.dbt"
```

Then parse the project without executing SQL:

```powershell
dbt parse --project-dir dbt --profiles-dir "$HOME\.dbt"
```

### 5. Build the transformation graph

Run the complete Phase 5 graph:

```powershell
dbt build --project-dir dbt --profiles-dir "$HOME\.dbt"
```

`dbt build` runs models and tests in dependency order. It is preferred here over separate
`dbt run` then `dbt test` commands because a model failure stops dependent work from being
reported as successful.

## Expected build shape

The Phase 5 graph is:

```text
raw.customers ------> stg_customers -----> dim_customer
                                           ^
                                           |
raw.accounts -------> stg_accounts ------> dim_account
                                           ^
                                           |
raw.merchants ------> stg_merchants -----> dim_merchant
       
raw.transaction_events
          |
          v
   stg_transactions
          |
          v
int_customer_transactions -----> fact_transactions
          |                         ^
          +--> int_daily_transactions|
          +--> int_fraud_candidates |
                                    |
                         dim_date --+
```

Physical output schemas:

```text
staging.stg_customers
staging.stg_accounts
staging.stg_merchants
staging.stg_transactions
staging.int_customer_transactions
staging.int_daily_transactions
staging.int_fraud_candidates

analytics.dim_customer
analytics.dim_account
analytics.dim_merchant
analytics.dim_date
analytics.fact_transactions
```

## Verification queries

After a successful build, connect with `psql` and run:

```sql
select count(*) from staging.stg_customers;
select count(*) from staging.stg_accounts;
select count(*) from staging.stg_merchants;
select count(*) from staging.stg_transactions;

select count(*) from analytics.dim_customer;
select count(*) from analytics.dim_account;
select count(*) from analytics.dim_merchant;
select count(*) from analytics.dim_date;
select count(*) from analytics.fact_transactions;
```

For the current dataset, the first eight entity counts should correspond to the source
cardinalities except `dim_date`, whose count is the number of calendar dates represented by
the transactions. `fact_transactions` should contain one row per valid raw transaction.

Do not hard-code a claim about the exact `dim_date` count without running the query, because
the model is intentionally derived from the actual transaction date range.

## Testing strategy

Phase 5 uses dbt schema tests for:

```text
not_null
unique
relationships
accepted_values
```

These tests protect key properties of the analytical contract:

- keys are populated
- entity grains remain unique
- foreign-key relationships remain intact across dimensions and facts
- controlled domains such as currency and transaction status stay valid

## What Phase 5 does not do

It does not:

- detect fraud alerts
- assign risk scores
- create `fact_fraud_alerts`
- add a scheduler
- add Airflow
- add Docker
- add Spark
- create an incremental model without a measured need
- introduce a package such as `dbt_utils`

These are deferred because they either belong to later phases or are unnecessary for the
current local workload.

## Verification honesty

The Phase 5 dbt SQL/YAML project has been statically checked in the build environment, but
actual `dbt debug`, `dbt parse`, `dbt build`, and live PostgreSQL test execution require the
native Windows development environment. The first live verification should therefore be
performed on the same machine that hosts PostgreSQL 17.2 and Kafka 3.9.2.
