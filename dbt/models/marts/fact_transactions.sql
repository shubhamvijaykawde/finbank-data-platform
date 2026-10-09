{{ config(
    pre_hook=[
        "alter table if exists {{ this }} drop constraint if exists pk_fact_transactions",
        "alter table if exists {{ this }} drop constraint if exists uq_fact_transactions_event_id",
        "alter table if exists {{ this }} drop constraint if exists uq_fact_transactions_transaction_id"
    ],
    post_hook=[
        "alter table {{ this }} add constraint pk_fact_transactions primary key (transaction_sk)",
        "alter table {{ this }} add constraint uq_fact_transactions_event_id unique (event_id)",
        "alter table {{ this }} add constraint uq_fact_transactions_transaction_id unique (transaction_id)"
    ]
) }}

select
    md5('transaction:' || t.transaction_id) as transaction_sk,
    t.event_id,
    t.transaction_id,
    d.date_sk,
    c.customer_sk,
    a.account_sk,
    m.merchant_sk,
    t.transaction_timestamp,
    t.currency,
    t.transaction_type,
    t.amount,
    t.transaction_country,
    t.transaction_city,
    t.payment_method,
    t.status,
    t.kafka_topic,
    t.kafka_partition,
    t.kafka_offset,
    t.ingested_at
from {{ ref('int_customer_transactions') }} t
inner join {{ ref('dim_date') }} d
    on d.full_date = t.transaction_date
inner join {{ ref('dim_customer') }} c
    on c.customer_id = t.customer_id
inner join {{ ref('dim_account') }} a
    on a.account_id = t.account_id
inner join {{ ref('dim_merchant') }} m
    on m.merchant_id = t.merchant_id
