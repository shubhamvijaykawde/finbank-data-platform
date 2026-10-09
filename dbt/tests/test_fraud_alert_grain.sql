select
    transaction_id,
    count(*) as alert_rows
from {{ ref('fact_fraud_alerts') }}
group by transaction_id
having count(*) != 1
