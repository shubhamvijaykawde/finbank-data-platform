# Phase 6 — Analytical Data Modeling

## Objective

Phase 6 formalizes the analytical model built in Phase 5. The objective is not to add another technology; it is to make the model's grain, keys, relationships, and business meaning explicit and enforceable.

The model is a small star schema centered on `analytics.fact_transactions` and surrounded by reusable dimensions. dbt's current best-practice guidance emphasizes clear model grain, tested primary-key assumptions, source isolation, and intentional information architecture; FinBank applies those ideas without introducing a larger warehouse platform.

## Local architecture

```text
raw PostgreSQL tables
        |
        v
staging views
        |
        v
intermediate views
        |
        v
+-------------------------------+
|         analytics              |
|                               |
| dim_customer   dim_account    |
| dim_merchant   dim_date       |
|               \   | /        |
|            fact_transactions  |
+-------------------------------+
```

No new runtime infrastructure is required. Everything continues to execute locally against native PostgreSQL 17.2 and native Windows dbt.

## The model's business process

The modeled business process is **bank transaction activity**.

The central fact represents the event of one valid transaction being accepted into the raw ingestion layer. The dimensions describe the customer, account, merchant, and calendar date associated with that transaction.

## Grain

Grain answers the question: **what exactly does one row represent?**

| Model | Grain | Why it matters |
|---|---|---|
| `dim_customer` | one row per customer | customer attributes can be reused across many transactions |
| `dim_account` | one row per account | account-level context joins cleanly to transaction facts |
| `dim_merchant` | one row per merchant | merchant attributes are reusable analytics dimensions |
| `dim_date` | one row per calendar date | common date grouping/filtering without repeating date logic |
| `fact_transactions` | one row per valid transaction ID | defines the atomic business process being analyzed |

The fact grain is the most important modeling decision. Every measure and dimension in `fact_transactions` must be interpreted at that grain.

## Facts versus dimensions

### Fact

`fact_transactions` stores the measurable business event:

- transaction amount
- transaction timestamp
- transaction status
- currency
- transaction type

It also stores foreign keys that connect the event to descriptive context.

### Dimensions

Dimensions answer questions about the event:

- Who? → `dim_customer`
- Which account? → `dim_account`
- Which merchant? → `dim_merchant`
- Which calendar date? → `dim_date`

The result is intentionally shaped for analytical queries rather than operational transaction processing.

## Keys

### Natural/business keys

Source identifiers such as `customer_id`, `account_id`, `merchant_id`, and `transaction_id` are natural/business keys. They retain traceability back to the source system and make reconciliation possible.

### Surrogate keys

The dimensions and fact also have deterministic warehouse keys:

```sql
md5('customer:' || customer_id)
md5('account:' || account_id)
md5('merchant:' || merchant_id)
md5('transaction:' || transaction_id)
```

`date_sk` is an integer `YYYYMMDD` key.

The namespace prefix makes the intent explicit and avoids reusing the same hash expression across entity types. These are warehouse identifiers, not security hashes.

### Primary keys

Phase 6 adds PostgreSQL primary-key constraints to the surrogate keys:

```text
dim_customer.customer_sk
dim_account.account_sk
dim_merchant.merchant_sk
dim_date.date_sk
fact_transactions.transaction_sk
```

### Foreign keys

The star-schema relationships are represented as foreign-key semantics in the dbt model and verified with `relationships` tests:

```text
dim_account.customer_sk -> dim_customer.customer_sk
fact_transactions.customer_sk -> dim_customer.customer_sk
fact_transactions.account_sk  -> dim_account.account_sk
fact_transactions.merchant_sk -> dim_merchant.merchant_sk
fact_transactions.date_sk     -> dim_date.date_sk
```

FinBank deliberately does **not** add physical PostgreSQL foreign-key constraints between dbt-managed mart tables. dbt rebuilds table materializations, and hard cross-table constraints create deployment coupling because a referenced dimension cannot simply be dropped/recreated while a fact table references it. The `relationships` tests provide the model-level referential-integrity check without imposing that rebuild dependency.

Physical primary-key and unique constraints are still applied to each mart table independently.

## Normalization versus denormalization

FinBank uses different modeling choices at different layers.

### Operational/raw layer

The raw data is closer to a normalized relational structure:

```text
customers
   |
accounts
   |
transactions ---- merchants
```

This reduces repeated descriptive attributes and preserves source relationships.

### Analytical layer

The `analytics` schema is intentionally denormalized into a star schema. The fact table joins directly to customer, account, merchant, and date dimensions.

This trades some repeated dimensional context for simpler analytical joins and clearer business semantics.

This is not an assertion that denormalization is always faster; the decision is made to optimize for understandable BI queries and a small analytical workload.

## Why not snowflake the dimensions?

For example, a merchant hierarchy could theoretically be split into separate merchant/category/country tables. FinBank keeps those descriptive attributes together in `dim_merchant` because the dataset is small and the expected questions are straightforward. This avoids unnecessary joins for dashboard users.

A more complex dimension hierarchy would become reasonable when the business process, ownership boundaries, or attribute history actually justify it.

## Additivity and currency

`amount` is additive only within a common currency.

For example:

```text
EUR 100 + EUR 50 = EUR 150
```

but:

```text
EUR 100 + GBP 50
```

should not be presented as a meaningful total without an explicit foreign-exchange conversion rule.

FinBank therefore preserves `currency` on the fact instead of inventing an FX model. Later dashboard queries should group or filter by currency unless an explicit FX normalization step is added.

## Transaction status

`status` remains a fact-level attribute because it describes the state of the transaction event.

Analytical queries should make their business definition explicit. For example:

- transaction count may count all statuses
- settled transaction value might count only `completed`
- failure rate should filter `failed`

The dashboard phase must not silently treat these as equivalent measures.

## Kafka provenance

`fact_transactions` retains:

```text
kafka_topic
kafka_partition
kafka_offset
ingested_at
```

These are operational lineage attributes, not business dimensions. Keeping them in the fact supports traceability from an analytical record back to the event-stream ingestion path.

## Query patterns the model supports

### Customer transaction value

```sql
select
    c.customer_segment,
    f.currency,
    sum(f.amount) as transaction_value
from analytics.fact_transactions f
join analytics.dim_customer c
  on c.customer_sk = f.customer_sk
group by
    c.customer_segment,
    f.currency;
```

### Daily transaction activity

```sql
select
    d.full_date,
    f.currency,
    count(*) as transaction_count,
    sum(f.amount) as transaction_value
from analytics.fact_transactions f
join analytics.dim_date d
  on d.date_sk = f.date_sk
group by
    d.full_date,
    f.currency
order by d.full_date;
```

### Merchant category analysis

```sql
select
    m.merchant_category,
    f.currency,
    count(*) as transaction_count,
    sum(f.amount) as transaction_value
from analytics.fact_transactions f
join analytics.dim_merchant m
  on m.merchant_sk = f.merchant_sk
group by
    m.merchant_category,
    f.currency;
```

The model makes these queries readable without rebuilding operational joins.

## Phase 6 constraints and tests

Phase 6 uses both dbt tests and PostgreSQL constraints:

```text
dbt not_null / unique / relationships
                 +
PostgreSQL PK / UNIQUE constraints
```

Foreign-key relationships remain logical/model-level constraints through dbt tests rather than physical cross-table PostgreSQL constraints, specifically to avoid unnecessary rebuild coupling between dbt-managed relations.

It also adds singular dbt tests for:

- fact transaction grain
- date-dimension continuity
- account/customer identity consistency

The goal is defense in depth: the model's assumptions are documented, tested, and physically represented where appropriate.


## Reliability fixes captured after verification

Phase 6 verification exposed two implementation regressions. Both are now fixed and covered by regression checks.

### Kafka provenance projection

`int_customer_transactions` must project `kafka_topic`, `kafka_partition`, `kafka_offset`, and `ingested_at` from `stg_transactions`. The fact model depends on these fields to preserve ingestion provenance.

### Idempotent mart constraint hooks

Every mart model uses a two-hook pattern for each project-managed PK/UNIQUE constraint:

```sql
pre_hook=[
    "alter table if exists {{ this }} drop constraint if exists <constraint_name>"
],
post_hook=[
    "alter table {{ this }} add constraint <constraint_name> ..."
]
```

The DROP must run in the `pre_hook`, before dbt-postgres performs its table rename/swap. During the swap, the old table can become a `__dbt_backup` relation and its constraints retain their names. A DROP in the `post_hook` targets the new table and cannot release a constraint name still owned by the backup relation. The pre-hook therefore prevents the repeated-build failure at its source.

A live regression test is available as `tests/test_dbt_build_twice.py`. Set `FINBANK_RUN_DBT_INTEGRATION_TESTS=1` in an environment with a working native PostgreSQL/dbt profile, then run:

```powershell
$env:FINBANK_RUN_DBT_INTEGRATION_TESTS="1"
pytest -q tests/test_dbt_build_twice.py
```

The test invokes `dbt build` twice consecutively and fails if either build returns a non-zero exit code.

## Verification

After the existing native Windows Phase 5 environment is active:

```powershell
dbt debug --project-dir dbt --profiles-dir "$HOME\.dbt"
dbt parse --project-dir dbt --profiles-dir "$HOME\.dbt"
dbt build --project-dir dbt --profiles-dir "$HOME\.dbt"
```

Then run:

```text
sql/006_data_model_verification.sql
```

For the live regression test, enable the integration test explicitly after
`dbt debug` succeeds:

```powershell
$env:FINBANK_RUN_DBT_INTEGRATION_TESTS="1"
pytest -q tests/test_dbt_build_twice.py
```

This executes `dbt build` twice in the same PostgreSQL environment. The
second build is the important assertion: it protects the mart post-hooks
against reintroducing the duplicate-constraint failure.

Expected checks:

- all five analytics relations exist
- primary-key and unique constraints are present; referential relationships pass dbt tests
- mart constraint configuration uses a defensive pre-hook DROP CONSTRAINT IF EXISTS followed by a post-hook ADD CONSTRAINT pattern
- running `dbt build` twice consecutively succeeds; this is the regression test for idempotent mart post-hooks
- fact grain query returns zero rows
- account/customer consistency query returns zero rows
- date-continuity query returns zero rows

## Why this is Phase 6 rather than a new infrastructure phase

The data model already existed structurally after Phase 5. Phase 6 makes the model explicit as a business-facing contract: its grains, keys, relationships, additive behavior, and analytical query patterns are documented and independently testable.

No additional infrastructure is introduced.
