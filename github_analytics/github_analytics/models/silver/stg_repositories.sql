-- models / silver / stg_repositories .sql

{{ config (
materialized ='view'
) }}

with source as (
select * from {{ source ('bronze', 'raw_repositories') }}
),
cleaned as (
select 

    full_name as repo_id, 

    cast(created_at as timestamp) as created_at,
    cast(updated_at as timestamp) as updated_at,
    cast(pushed_at as timestamp) as pushed_at,

    cast(stargazers_count as integer) as stargazers_count,
    cast(watchers_count as integer) as watchers_count,
    cast(open_issues_count as integer) as open_issues_count,
    cast(forks_count as integer) as forks_count,
    cast(size as integer) as size,

    coalesce(description, 'No description') as description,
    coalesce(language, 'Unknown') as language,

    datediff('day', cast(created_at as timestamp), current_timestamp) as repo_age_days,

    archived

from source 
where archived = false 

)

select * from cleaned