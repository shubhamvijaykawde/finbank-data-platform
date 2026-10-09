with thresholds as (
    select
        3000.00::numeric as large_transaction_amount,
        10.0::numeric as burst_minutes,
        60.0::numeric as impossible_travel_minutes,
        5.0::numeric as unusual_spending_ratio,
        1500.00::numeric as failed_attempts_large_amount,
        30.0::numeric as failed_attempt_window_minutes,
        2000.00::numeric as suspicious_merchant_amount
),
base as (
    select
        t.*,
        thresholds.large_transaction_amount,
        thresholds.burst_minutes,
        thresholds.impossible_travel_minutes,
        thresholds.unusual_spending_ratio,
        thresholds.failed_attempts_large_amount,
        thresholds.failed_attempt_window_minutes,
        thresholds.suspicious_merchant_amount
    from {{ ref('int_customer_transactions') }} t
    cross join thresholds
),
rule_evaluation as (
    select
        *,
        (
            amount > large_transaction_amount
        ) as large_transaction_flag,
        (
            minutes_since_previous_transaction is not null
            and minutes_since_previous_transaction <= burst_minutes
        ) as transaction_burst_flag,
        (
            minutes_since_previous_transaction is not null
            and minutes_since_previous_transaction <= impossible_travel_minutes
            and previous_transaction_country is not null
            and transaction_country <> previous_transaction_country
        ) as impossible_travel_flag,
        (
            amount_to_prior_customer_avg is not null
            and amount_to_prior_customer_avg >= unusual_spending_ratio
        ) as unusual_spending_flag,
        (
            status = 'completed'
            and amount >= failed_attempts_large_amount
            and previous_transaction_status = 'failed'
            and previous_previous_transaction_status = 'failed'
            and minutes_since_previous_transaction is not null
            and minutes_since_previous_transaction <= failed_attempt_window_minutes
        ) as failed_attempts_before_large_success_flag,
        (
            amount >= suspicious_merchant_amount
            and merchant_category in ('electronics', 'travel', 'entertainment')
        ) as suspicious_merchant_behavior_flag
    from base
),
scored as (
    select
        *,
        least(
            100,
            case when large_transaction_flag then 40 else 0 end
            + case when transaction_burst_flag then 20 else 0 end
            + case when impossible_travel_flag then 35 else 0 end
            + case when unusual_spending_flag then 25 else 0 end
            + case when failed_attempts_before_large_success_flag then 30 else 0 end
            + case when suspicious_merchant_behavior_flag then 15 else 0 end
        ) as risk_score
    from rule_evaluation
)
select
    transaction_id,
    customer_id,
    transaction_timestamp,
    transaction_date,
    amount,
    currency,
    transaction_country,
    customer_home_country,
    merchant_category,
    status,
    previous_transaction_timestamp,
    previous_transaction_country,
    previous_transaction_status,
    previous_previous_transaction_status,
    minutes_since_previous_transaction,
    prior_customer_avg_amount,
    amount_to_prior_customer_avg,
    large_transaction_flag,
    transaction_burst_flag,
    impossible_travel_flag,
    unusual_spending_flag,
    failed_attempts_before_large_success_flag,
    suspicious_merchant_behavior_flag,
    risk_score,
    case
        when risk_score >= 70 then 'HIGH'
        when risk_score >= 40 then 'MEDIUM'
        else 'LOW'
    end as severity,
    concat_ws(
        '; ',
        case
            when large_transaction_flag then
                'Large transaction: amount exceeds the EUR/GBP 3,000 analytical threshold'
        end,
        case
            when transaction_burst_flag then
                'Transaction burst: previous customer transaction occurred within 10 minutes'
        end,
        case
            when impossible_travel_flag then
                'Impossible travel: transaction country changed within 60 minutes'
        end,
        case
            when unusual_spending_flag then
                'Unusual spending: amount is at least 5x the customer prior average'
        end,
        case
            when failed_attempts_before_large_success_flag then
                'Failed attempts: two consecutive failed transactions preceded a large successful transaction within 30 minutes'
        end,
        case
            when suspicious_merchant_behavior_flag then
                'Suspicious merchant behavior: high-value activity occurred in a monitored merchant category'
        end
    ) as reason
from scored
