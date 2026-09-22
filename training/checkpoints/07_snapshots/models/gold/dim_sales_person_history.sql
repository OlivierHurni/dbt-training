-- Store assignments of every sales person over time, built on the snapshot (SCD type 2).
select
    sales_person_id,
    full_name,
    store_id,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    dbt_valid_to is null as is_current
from (
    select
        *,
        concat_ws(' ', first_name, last_name) as full_name
    from {{ ref('snap_sales_persons') }}
)
