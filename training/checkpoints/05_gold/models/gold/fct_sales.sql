-- Grain: one row per order line.
with orders as (

    select * from {{ ref('stg_sales_orders') }}

),

lines as (

    select * from {{ ref('stg_sales_order_lines') }}

),

products as (

    select product_id, unit_cost from {{ ref('stg_products') }}

)

select
    l.order_line_id,
    l.order_id,
    o.order_date,
    o.order_day,
    o.customer_id,
    o.store_id,
    o.sales_person_id,
    l.product_id,
    o.channel,
    o.payment_method,
    o.status as order_status,
    l.quantity,
    l.unit_price,
    l.discount_pct,
    round(l.quantity * l.unit_price, 2) as gross_amount,
    round(l.quantity * l.unit_price, 2) - {{ net_amount('l.quantity', 'l.unit_price', 'l.discount_pct') }} as discount_amount,
    {{ net_amount('l.quantity', 'l.unit_price', 'l.discount_pct') }} as net_amount,
    round(l.quantity * p.unit_cost, 2) as cost_amount,
    {{ net_amount('l.quantity', 'l.unit_price', 'l.discount_pct') }} - round(l.quantity * p.unit_cost, 2) as margin_amount,
    o.updated_at as order_updated_at
from lines as l
inner join orders as o
    on l.order_id = o.order_id
left join products as p
    on l.product_id = p.product_id
