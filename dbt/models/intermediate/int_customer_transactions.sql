with transaction_context as (
    select
        t.event_id,
        t.transaction_id,
        t.transaction_timestamp,
        t.transaction_date,
        t.customer_id,
        t.account_id,
        t.merchant_id,
        t.kafka_topic,
        t.kafka_partition,
        t.kafka_offset,
        t.ingested_at,
        t.amount,
        t.currency,
        t.transaction_type,
        t.country as transaction_country,
        t.city as transaction_city,
        t.payment_method,
        t.status,
        c.customer_segment,
        c.country as customer_home_country,
        c.city as customer_home_city,
        c.registration_date,
        a.account_type,
        m.merchant_category,
        row_number() over (
            partition by t.customer_id
            order by t.transaction_timestamp, t.transaction_id
        ) as customer_transaction_sequence,
        lag(t.transaction_timestamp) over (
            partition by t.customer_id
            order by t.transaction_timestamp, t.transaction_id
        ) as previous_transaction_timestamp,
        lag(t.country) over (
            partition by t.customer_id
            order by t.transaction_timestamp, t.transaction_id
        ) as previous_transaction_country,
        lag(t.status) over (
            partition by t.customer_id
            order by t.transaction_timestamp, t.transaction_id
        ) as previous_transaction_status,
        lag(t.status, 2) over (
            partition by t.customer_id
            order by t.transaction_timestamp, t.transaction_id
        ) as previous_previous_transaction_status,
        avg(t.amount) over (
            partition by t.customer_id
            order by t.transaction_timestamp, t.transaction_id
            rows between unbounded preceding and 1 preceding
        ) as prior_customer_avg_amount
    from {{ ref('stg_transactions') }} t
    inner join {{ ref('stg_customers') }} c
        on c.customer_id = t.customer_id
    inner join {{ ref('stg_accounts') }} a
        on a.account_id = t.account_id
       and a.customer_id = t.customer_id
    inner join {{ ref('stg_merchants') }} m
        on m.merchant_id = t.merchant_id
)

select
    *,
    (transaction_country = customer_home_country) as is_domestic_transaction,
    case
        when previous_transaction_timestamp is null then null
        else extract(
            epoch from (transaction_timestamp - previous_transaction_timestamp)
        ) / 60.0
    end as minutes_since_previous_transaction,
    case
        when prior_customer_avg_amount is null or prior_customer_avg_amount = 0
            then null
        else amount / prior_customer_avg_amount
    end as amount_to_prior_customer_avg
from transaction_context
