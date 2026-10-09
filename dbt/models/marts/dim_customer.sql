{{ config(
    pre_hook=[
        "alter table if exists {{ this }} drop constraint if exists pk_dim_customer",
        "alter table if exists {{ this }} drop constraint if exists uq_dim_customer_customer_id"
    ],
    post_hook=[
        "alter table {{ this }} add constraint pk_dim_customer primary key (customer_sk)",
        "alter table {{ this }} add constraint uq_dim_customer_customer_id unique (customer_id)"
    ]
) }}

select
    md5('customer:' || customer_id) as customer_sk,
    customer_id,
    first_name,
    last_name,
    date_of_birth,
    country,
    city,
    registration_date,
    customer_segment
from {{ ref('stg_customers') }}
