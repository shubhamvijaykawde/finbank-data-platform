select
    trim(merchant_id) as merchant_id,
    trim(merchant_name) as merchant_name,
    trim(merchant_category) as merchant_category,
    trim(country) as country,
    trim(city) as city
from {{ source('finbank_raw', 'merchants') }}
