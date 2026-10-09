select
    transaction_date,
    currency,
    transaction_country,
    status,
    count(*) as transaction_count,
    count(distinct customer_id) as active_customers,
    sum(amount) as total_transaction_value,
    avg(amount) as average_transaction_amount
from {{ ref('int_customer_transactions') }}
group by
    transaction_date,
    currency,
    transaction_country,
    status
