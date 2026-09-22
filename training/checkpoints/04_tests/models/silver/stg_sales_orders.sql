-- Cleans the raw orders:
--   * removes exact duplicates (keeps the most recently updated version of each order)
--   * normalizes the status (COMPLETED -> completed)
--   * flags orders pointing to a customer that does not exist and clears the reference
with orders as (

    select
        *,
        row_number() over (
            partition by order_id
            order by updated_at desc, _ingested_at desc
        ) as row_num
    from {{ source('bronze', 'sales_orders') }}

),

deduplicated as (

    select * from orders where row_num = 1

)

select
    o.order_id,
    o.order_date,
    cast(o.order_date as date) as order_day,
    c.customer_id,
    o.customer_id is not null and c.customer_id is null as is_orphan_customer,
    o.store_id,
    o.sales_person_id,
    o.channel,
    o.payment_method,
    lower(trim(o.status)) as status,
    o.created_at,
    o.updated_at,
    o._batch_id
from deduplicated as o
left join {{ ref('stg_customers') }} as c
    on o.customer_id = c.customer_id
