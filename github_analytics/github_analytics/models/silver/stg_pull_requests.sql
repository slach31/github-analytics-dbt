{{ config(
materialized='view'
) }}

with source as (
    select * from {{ source('bronze', 'raw_pull_requests') }}
),

cleaned as (
    select

        cast(pr_number as integer) as pr_number,

        cast(created_at as timestamp) as created_at,
        cast(merged_at as timestamp) as merged_at,
        cast(closed_at as timestamp) as closed_at,

        (merged_at is not null) as is_merged,
        cast(draft as boolean) as is_draft,

        case
            when merged_at is not null then datediff('hour', created_at, merged_at)
            when closed_at is not null then datediff('hour', created_at, closed_at)
            else null
        end as time_to_close_hours

    from source
    where pr_number is not null
)

select * from cleaned