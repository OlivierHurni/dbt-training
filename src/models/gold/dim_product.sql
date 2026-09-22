select
    product_id,
    product_name,
    category,
    brand,
    list_price,
    unit_cost,
    round(list_price - unit_cost, 2) as unit_margin,
    is_active
from {{ ref('stg_products') }}
