{{ config(
    pre_hook=[
        "alter table if exists {{ this }} drop constraint if exists pk_dim_merchant",
        "alter table if exists {{ this }} drop constraint if exists uq_dim_merchant_merchant_id"
    ],
    post_hook=[
        "alter table {{ this }} add constraint pk_dim_merchant primary key (merchant_sk)",
        "alter table {{ this }} add constraint uq_dim_merchant_merchant_id unique (merchant_id)"
    ]
) }}

select
    md5('merchant:' || merchant_id) as merchant_sk,
    merchant_id,
    merchant_name,
    merchant_category,
    country,
    city
from {{ ref('stg_merchants') }}
