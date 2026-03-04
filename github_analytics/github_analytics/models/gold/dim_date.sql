{{ config(materialized='table') }}

with date_spine as (

    select
        unnest(
            generate_series(
                current_date - interval '5 years',
                current_date,
                interval '1 day'
            )
        )::date as full_date

),

enriched as (

    select
        -- surrogate key YYYYMMDD as integer
        cast(strftime(full_date, '%Y%m%d') as integer) as date_id,

        full_date,

        -- temporal extractions
        extract(year from full_date)  as year,
        extract(month from full_date) as month,
        extract(week from full_date)  as week_of_year,

        -- DuckDB: dayofweek() returns 0=Sunday .. 6=Saturday
        dayofweek(full_date)          as day_of_week,

        -- readable names
        strftime(full_date, '%A')     as day_name,
        strftime(full_date, '%B')     as month_name,

        -- weekend flag (0 Sunday, 6 Saturday)
        (dayofweek(full_date) in (0, 6)) as is_weekend,

        -- quarter 1..4
        extract(quarter from full_date) as quarter

    from date_spine

)

select *
from enriched
order by full_date