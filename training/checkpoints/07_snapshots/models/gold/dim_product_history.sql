-- Price history of every product, built on the snapshot (SCD type 2).
select
    product_id,
    product_name,
    category,
    brand,
    list_price,
    unit_cost,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    dbt_valid_to is null as is_current
from {{ ref('snap_products') }}
