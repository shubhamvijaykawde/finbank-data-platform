select
    transaction_id,
    risk_score
from {{ ref('int_fraud_candidates') }}
where risk_score < 0
   or risk_score > 100
