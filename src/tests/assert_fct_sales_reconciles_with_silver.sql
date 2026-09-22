-- Singular test: returns a row (= fails) if the total net amount in the gold fact table
-- differs from the total recomputed directly from the silver tables.
with gold as (

    select sum(net_amount) as net_amount from {{ ref('fct_sales') }}

),

silver as (

    select sum({{ net_amount('l.quantity', 'l.unit_price', 'l.discount_pct') }}) as net_amount
    from {{ ref('stg_sales_order_lines') }} as l
    inner join {{ ref('stg_sales_orders') }} as o
        on l.order_id = o.order_id

)

select
    gold.net_amount as gold_net_amount,
    silver.net_amount as silver_net_amount
from gold
cross join silver
where abs(gold.net_amount - silver.net_amount) > 0.01
