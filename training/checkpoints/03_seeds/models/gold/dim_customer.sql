-- Customers enriched with the country attributes from the `countries` seed.
select
    c.customer_id,
    c.full_name,
    c.email,
    c.city,
    c.country_code,
    co.country_name,
    co.continent,
    co.is_eu,
    c.loyalty_tier,
    c.created_at
from {{ ref('stg_customers') }} as c
left join {{ ref('countries') }} as co
    on c.country_code = co.country_code
