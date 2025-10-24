{{
    config(
        materialized='table',
        tags=['dimension', 'cross-db']
    )
}}

-- Using dbt_date package for date spine generation
{{ dbt_date.get_date_dimension(var('start_date'), var('end_date')) }}
