-- Cleans the raw order lines:
--   * removes exact duplicates
--   * drops lines with a non-positive quantity or with a product that does not exist
with lines as (

    select
        *,
        row_number() over (
            partition by order_line_id
            order by _ingested_at desc
        ) as row_num
    from {{ source('bronze', 'sales_order_lines') }}

)

select
    l.order_line_id,
    l.order_id,
    l.product_id,
    l.quantity,
    l.unit_price,
    l.discount_pct,
    l._batch_id
from lines as l
inner join {{ ref('stg_products') }} as p
    on l.product_id = p.product_id
where l.row_num = 1
  and l.quantity > 0
