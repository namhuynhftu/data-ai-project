{% macro generate_date_spine(start_date, end_date) %}

with date_spine as (
    {{ dbt.date_spine(
        datepart="day",
        start_date="cast('" ~ start_date ~ "' as date)",
        end_date="cast('" ~ end_date ~ "' as date)"
    ) }}
)

select
    date_day,
    {{ dbt.date_trunc('week', 'date_day') }} as week_start,
    {{ dbt.date_trunc('month', 'date_day') }} as month_start,
    {{ dbt.date_trunc('quarter', 'date_day') }} as quarter_start,
    {{ dbt.date_trunc('year', 'date_day') }} as year_start,
    extract(year from date_day) as year_number,
    extract(quarter from date_day) as quarter_number,
    extract(month from date_day) as month_number,
    extract(day from date_day) as day_of_month,
    dayofweek(date_day) as day_of_week,
    dayofyear(date_day) as day_of_year,
    case when dayofweek(date_day) in (0, 6) then true else false end as is_weekend
from date_spine

{% endmacro %}
