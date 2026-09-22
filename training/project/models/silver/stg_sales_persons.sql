select
    sales_person_id,
    first_name,
    last_name,
    concat_ws(' ', first_name, last_name) as full_name,
    lower(email) as email,
    store_id,
    hire_date,
    updated_at
from {{ source('bronze', 'sales_persons') }}
