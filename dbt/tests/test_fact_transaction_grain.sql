-- Grain contract: one row per business transaction ID.
select
    transaction_id,
    count(*) as row_count
from {{ ref('fact_transactions') }}
group by transaction_id
having count(*) <> 1
