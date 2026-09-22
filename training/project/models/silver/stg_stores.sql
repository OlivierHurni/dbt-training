select
    store_id,
    store_name,
    city,
    canton,
    store_type,
    store_type = 'online' as is_online,
    opened_date
from {{ source('bronze', 'stores') }}
