select
    order_id,
    order_date,
    cast(order_date as date) as order_day,
    customer_id,
    store_id,
    sales_person_id,
    channel,
    payment_method,
    status,
    created_at,
    updated_at,
    _batch_id
from {{ source('bronze', 'sales_orders') }}
