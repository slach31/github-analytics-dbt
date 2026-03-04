{{ config(materialized='table') }}

with from_commits as (
    select
        author_login as login,
        repo_id,
        author_date as activity_date
    from {{ ref('stg_commits') }}
    where author_login is not null
),

from_prs as (
    select
        user_login as login,
        repo_full_name as repo_id,
        created_at as activity_date
    from {{ ref('stg_pull_requests') }}
    where user_login is not null
),

contributors as (
    select * from from_commits
    union all
    select * from from_prs
),

aggregated as (
    select
        login as contributor_id,
        login,
        min(activity_date) as first_contribution_at,
        count(distinct repo_id) as repos_contributed_to,
        count(*) as total_activities
    from contributors
    where login != 'Unknown'
    group by login
)

select * from aggregated