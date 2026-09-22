select
    order_line_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    discount_pct,
    _batch_id
from {{ source('bronze', 'sales_order_lines') }}
