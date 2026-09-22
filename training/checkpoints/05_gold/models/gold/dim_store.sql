select
    store_id,
    store_name,
    city,
    canton,
    store_type,
    is_online,
    opened_date
from {{ ref('stg_stores') }}
