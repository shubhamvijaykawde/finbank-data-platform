{{ config(
    pre_hook=[
        "alter table if exists {{ this }} drop constraint if exists pk_dim_date",
        "alter table if exists {{ this }} drop constraint if exists uq_dim_date_full_date"
    ],
    post_hook=[
        "alter table {{ this }} add constraint pk_dim_date primary key (date_sk)",
        "alter table {{ this }} add constraint uq_dim_date_full_date unique (full_date)"
    ]
) }}

with bounds as (
    select
        min(transaction_date) as start_date,
        max(transaction_date) as end_date
    from {{ ref('stg_transactions') }}
),
calendar as (
    select
        generate_series(start_date, end_date, interval '1 day')::date as full_date
    from bounds
    where start_date is not null
      and end_date is not null
)

select
    to_char(full_date, 'YYYYMMDD')::integer as date_sk,
    full_date,
    extract(isodow from full_date)::integer as day_of_week,
    trim(to_char(full_date, 'Day')) as day_name,
    extract(week from full_date)::integer as week_of_year,
    extract(month from full_date)::integer as month_number,
    trim(to_char(full_date, 'Month')) as month_name,
    extract(quarter from full_date)::integer as quarter_number,
    extract(year from full_date)::integer as year_number,
    (extract(isodow from full_date) in (6, 7)) as is_weekend
from calendar
