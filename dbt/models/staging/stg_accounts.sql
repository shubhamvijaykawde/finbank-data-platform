select
    trim(account_id) as account_id,
    trim(customer_id) as customer_id,
    trim(account_type) as account_type,
    trim(currency) as currency,
    account_open_date,
    initial_balance::numeric(18, 2) as initial_balance
from {{ source('finbank_raw', 'accounts') }}
