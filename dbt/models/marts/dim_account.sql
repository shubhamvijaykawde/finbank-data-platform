{{ config(
    pre_hook=[
        "alter table if exists {{ this }} drop constraint if exists pk_dim_account",
        "alter table if exists {{ this }} drop constraint if exists uq_dim_account_account_id"
    ],
    post_hook=[
        "alter table {{ this }} add constraint pk_dim_account primary key (account_sk)",
        "alter table {{ this }} add constraint uq_dim_account_account_id unique (account_id)"
    ]
) }}

select
    md5('account:' || a.account_id) as account_sk,
    a.account_id,
    md5('customer:' || a.customer_id) as customer_sk,
    a.customer_id,
    a.account_type,
    a.currency,
    a.account_open_date,
    a.initial_balance
from {{ ref('stg_accounts') }} a
