{{ config(
    pre_hook=[
        "alter table if exists {{ this }} drop constraint if exists pk_fact_fraud_alerts",
        "alter table if exists {{ this }} drop constraint if exists uq_fact_fraud_alerts_alert_id",
        "alter table if exists {{ this }} drop constraint if exists uq_fact_fraud_alerts_transaction_id"
    ],
    post_hook=[
        "alter table {{ this }} add constraint pk_fact_fraud_alerts primary key (fraud_alert_id)",
        "alter table {{ this }} add constraint uq_fact_fraud_alerts_alert_id unique (fraud_alert_id)",
        "alter table {{ this }} add constraint uq_fact_fraud_alerts_transaction_id unique (transaction_id)"
    ]
) }}

select
    md5('fraud-alert:' || c.transaction_id) as fraud_alert_id,
    c.transaction_id,
    c.customer_id,
    c.risk_score,
    c.severity,
    c.reason,
    concat_ws(
        '; ',
        case when c.large_transaction_flag then 'LARGE_TRANSACTION' end,
        case when c.transaction_burst_flag then 'TRANSACTION_BURST' end,
        case when c.impossible_travel_flag then 'IMPOSSIBLE_TRAVEL' end,
        case when c.unusual_spending_flag then 'UNUSUAL_SPENDING' end,
        case when c.failed_attempts_before_large_success_flag then 'FAILED_ATTEMPTS_BEFORE_LARGE_SUCCESS' end,
        case when c.suspicious_merchant_behavior_flag then 'SUSPICIOUS_MERCHANT_BEHAVIOR' end
    ) as rule_triggered,
    c.amount,
    c.currency,
    c.transaction_timestamp,
    c.transaction_country,
    c.merchant_category,
    f.kafka_topic,
    f.kafka_partition,
    f.kafka_offset,
    f.ingested_at as created_at
from {{ ref('int_fraud_candidates') }} c
inner join {{ ref('fact_transactions') }} f
    on f.transaction_id = c.transaction_id
where c.risk_score > 0
