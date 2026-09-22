{{ config(severity='warn') }}

-- Singular test: every silver order should have at least one line in the fact table.
-- Expected to warn: an order whose only line was dirty in bronze (negative quantity,
-- unknown product) loses that line in silver and is left without any line.
select o.order_id
from {{ ref('stg_sales_orders') }} as o
left join {{ ref('fct_sales') }} as f
    on o.order_id = f.order_id
where f.order_id is null
