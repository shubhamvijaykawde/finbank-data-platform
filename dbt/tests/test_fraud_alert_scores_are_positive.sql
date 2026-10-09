select
    transaction_id,
    risk_score
from {{ ref('fact_fraud_alerts') }}
where risk_score <= 0
