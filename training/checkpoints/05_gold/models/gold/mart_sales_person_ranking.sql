-- Sales persons ranked by revenue, with the cumulative share of total revenue (the 80/20 view).
with revenue as (

    select
        sp.sales_person_id,
        sp.full_name,
        sp.store_name,
        count(distinct f.order_id) as orders,
        sum(f.net_amount) as net_revenue
    from {{ ref('fct_sales') }} as f
    inner join {{ ref('dim_sales_person') }} as sp
        on f.sales_person_id = sp.sales_person_id
    where f.order_status = 'completed'
    group by 1, 2, 3

)

select
    rank() over (order by net_revenue desc) as revenue_rank,
    sales_person_id,
    full_name,
    store_name,
    orders,
    net_revenue,
    round(net_revenue / sum(net_revenue) over (), 4) as revenue_share,
    round(sum(net_revenue) over (order by net_revenue desc) / sum(net_revenue) over (), 4) as cumulative_revenue_share
from revenue
