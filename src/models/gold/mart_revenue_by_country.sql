-- Revenue per customer country and continent (uses the `countries` seed through dim_customer).
select
    c.country_code,
    c.country_name,
    c.continent,
    c.is_eu,
    count(distinct f.customer_id) as customers,
    count(distinct f.order_id) as orders,
    sum(f.net_amount) as net_revenue
from {{ ref('fct_sales') }} as f
inner join {{ ref('dim_customer') }} as c
    on f.customer_id = c.customer_id
where f.order_status = 'completed'
group by 1, 2, 3, 4
