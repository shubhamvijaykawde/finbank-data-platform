-- A generated calendar dimension must not contain gaps between its first and last date.
with ordered_dates as (
    select
        full_date,
        lead(full_date) over (order by full_date) as next_date
    from {{ ref('dim_date') }}
)
select
    full_date,
    next_date
from ordered_dates
where next_date is not null
  and next_date <> full_date + interval '1 day'
