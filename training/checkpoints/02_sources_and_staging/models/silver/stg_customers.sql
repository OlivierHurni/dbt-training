select
    customer_id,
    first_name,
    last_name,
    concat_ws(' ', first_name, last_name) as full_name,
    lower(email) as email,
    city,
    upper(country_code) as country_code,
    loyalty_tier,
    created_at,
    updated_at
from {{ source('bronze', 'customers') }}
