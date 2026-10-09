-- An account must point to the same customer identity represented by its customer_sk.
select
    a.account_id,
    a.customer_id as account_customer_id,
    c.customer_id as dimension_customer_id
from {{ ref('dim_account') }} a
inner join {{ ref('dim_customer') }} c
    on c.customer_sk = a.customer_sk
where a.customer_id <> c.customer_id
