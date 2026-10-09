-- Singular dbt test: returns fraud alerts that have no rule evidence.
-- A valid alert must have both a non-empty rule_triggered value and
-- a non-empty explanation in reason.
select
    fraud_alert_id,
    transaction_id,
    rule_triggered,
    reason
from {{ ref('fact_fraud_alerts') }}
where rule_triggered is null
   or btrim(rule_triggered) = ''
   or reason is null
   or btrim(reason) = ''
