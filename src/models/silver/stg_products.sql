select
    product_id,
    trim(product_name) as product_name,
    category,
    brand,
    list_price,
    unit_cost,
    is_active,
    created_at,
    updated_at
from {{ source('bronze', 'products') }}
