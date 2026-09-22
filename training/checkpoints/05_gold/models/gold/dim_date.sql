-- Calendar dimension generated with the dbt_utils.date_spine macro.
with spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2024-01-01' as date)",
        end_date="cast('2029-01-01' as date)"
    ) }}

)

select
    cast(date_day as date) as date_day,
    year(date_day) as year,
    quarter(date_day) as quarter,
    month(date_day) as month,
    date_format(date_day, 'MMMM') as month_name,
    weekofyear(date_day) as week_of_year,
    weekday(date_day) + 1 as day_of_week,  -- 1 = Monday
    date_format(date_day, 'EEEE') as day_name,
    weekday(date_day) >= 5 as is_weekend,
    case
        when month(date_day) in (12, 1, 2) then 'winter'
        when month(date_day) in (3, 4, 5) then 'spring'
        when month(date_day) in (6, 7, 8) then 'summer'
        else 'autumn'
    end as season
from spine
