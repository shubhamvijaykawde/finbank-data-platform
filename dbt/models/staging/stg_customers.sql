select
    trim(customer_id) as customer_id,
    trim(first_name) as first_name,
    trim(last_name) as last_name,
    date_of_birth,
    trim(country) as country,
    trim(city) as city,
    registration_date,
    trim(customer_segment) as customer_segment
from {{ source('finbank_raw', 'customers') }}
