-- Monthly performance per store (completed orders only).
select
    date_trunc('month', f.order_day) as month,
    s.store_id,
    s.store_name,
    s.store_type,
    count(distinct f.order_id) as orders,
    sum(f.net_amount) as net_revenue,
    round(sum(f.net_amount) / count(distinct f.order_id), 2) as avg_basket,
    sum(f.margin_amount) as margin
from {{ ref('fct_sales') }} as f
inner join {{ ref('dim_store') }} as s
    on f.store_id = s.store_id
where f.order_status = 'completed'
group by 1, 2, 3, 4
