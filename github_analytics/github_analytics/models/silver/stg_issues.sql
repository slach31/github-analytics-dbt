{{ config(materialized='view') }}

with source as (
    select *
    from {{ source('bronze', 'raw_issues') }}
),

renamed as (
    select
        cast(issue_number as {{ dbt.type_int() }})      as issue_number,
        cast(title as {{ dbt.type_string() }})          as title,
        cast(state as {{ dbt.type_string() }})          as state,
        cast(created_at as {{ dbt.type_timestamp() }})  as created_at,
        cast(updated_at as {{ dbt.type_timestamp() }})  as updated_at,
        cast(closed_at  as {{ dbt.type_timestamp() }})  as closed_at,

        case
            when lower(cast(is_pull_request as {{ dbt.type_string() }})) in ('true','t','1','yes','y')
                then true
            else false
        end as is_pull_request

    from source
    where issue_number is not null
),

final as (
    select
        *,
        case
            when closed_at is not null then datediff('hour', created_at, closed_at)
            else null
        end as time_to_close_hours
    from renamed
)

select *
from final
where is_pull_request = false