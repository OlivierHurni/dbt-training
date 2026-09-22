select
    sp.sales_person_id,
    sp.full_name,
    sp.email,
    sp.hire_date,
    sp.store_id,
    s.store_name,
    s.canton as store_canton
from {{ ref('stg_sales_persons') }} as sp
left join {{ ref('stg_stores') }} as s
    on sp.store_id = s.store_id
