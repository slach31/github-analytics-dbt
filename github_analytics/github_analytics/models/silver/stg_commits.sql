-- models/silver/stg_commits.sql
{{ config(
materialized='view'
) }}

with source as (
    select * from {{ source('bronze', 'raw_commits') }}
),

cleaned as (
    select

        sha as commit_sha,
        repo_full_name as repo_id,

        coalesce(author_login, 'Unknown') as author_login,

        cast(author_date as timestamp) as author_date,
        cast(committer_date as timestamp) as committer_date,

        extract(dow from cast(author_date as timestamp)) as day_of_week,
        extract(hour from cast(author_date as timestamp)) as hour_of_day,

        substring(message, 1, 200) as message

    from source

    where sha is not null
)

select * from cleaned