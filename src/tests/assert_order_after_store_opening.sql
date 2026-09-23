-- Singular test: an order can not be placed before the store opened.
-- The test fails if it returns any row.
select
    o.order_id,
    o.order_day,
    s.opened_date
from {{ ref('stg_sales_orders') }} as o
inner join {{ ref('stg_stores') }} as s
    on o.store_id = s.store_id
where o.order_day < s.opened_date
