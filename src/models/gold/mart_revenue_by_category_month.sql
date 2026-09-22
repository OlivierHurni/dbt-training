-- Monthly revenue per product category (completed orders only): shows the ski / swimming seasonality.
select
    date_trunc('month', f.order_day) as month,
    p.category,
    count(distinct f.order_id) as orders,
    sum(f.quantity) as units_sold,
    sum(f.net_amount) as net_revenue,
    sum(f.margin_amount) as margin
from {{ ref('fct_sales') }} as f
inner join {{ ref('dim_product') }} as p
    on f.product_id = p.product_id
where f.order_status = 'completed'
group by 1, 2
