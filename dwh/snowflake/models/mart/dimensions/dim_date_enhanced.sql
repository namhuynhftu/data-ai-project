{{
    config(
        materialized='table',
        tags=['dimension', 'cross-db']
    )
}}

-- Using dbt cross-database macros for compatibility
with date_spine as (
    {{ generate_date_spine('2016-01-01', '2025-12-31') }}
)

select
    date_day as date_key,
    date_day,
    week_start,
    month_start,
    quarter_start,
    year_start,
    year_number,
    quarter_number,
    month_number,
    day_of_month,
    day_of_week,
    day_of_year,
    is_weekend,
    -- Cross-database date formatting
    {{ dbt.date_trunc('month', 'date_day') }} as first_day_of_month,
    {{ dbt.last_day('date_day', 'month') }} as last_day_of_month,
    case
        when month_number in (1, 2, 3) then 'Q1'
        when month_number in (4, 5, 6) then 'Q2'
        when month_number in (7, 8, 9) then 'Q3'
        else 'Q4'
    end as quarter_name,
    case
        when month_number = 1 then 'January'
        when month_number = 2 then 'February'
        when month_number = 3 then 'March'
        when month_number = 4 then 'April'
        when month_number = 5 then 'May'
        when month_number = 6 then 'June'
        when month_number = 7 then 'July'
        when month_number = 8 then 'August'
        when month_number = 9 then 'September'
        when month_number = 10 then 'October'
        when month_number = 11 then 'November'
        else 'December'
    end as month_name,
    case
        when day_of_week = 0 then 'Sunday'
        when day_of_week = 1 then 'Monday'
        when day_of_week = 2 then 'Tuesday'
        when day_of_week = 3 then 'Wednesday'
        when day_of_week = 4 then 'Thursday'
        when day_of_week = 5 then 'Friday'
        else 'Saturday'
    end as day_name
from date_spine
