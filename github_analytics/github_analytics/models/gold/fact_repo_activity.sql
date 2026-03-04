{{ config(materialized='table') }}

with daily_commits as (

    select
        repo_id,
        cast(author_date as date) as activity_date,
        count(*) as commits_count,
        count(distinct author_login) as unique_committers
    from {{ ref('stg_commits') }}
    group by repo_id, cast(author_date as date)

),

daily_prs as (

    select
        repo_full_name as repo_id,
        cast(created_at as date) as activity_date,
        count(*) as prs_opened,
        sum(case when is_merged then 1 else 0 end) as prs_merged,
        avg(time_to_close_hours) as avg_pr_close_hours
    from {{ ref('stg_pull_requests') }}
    group by repo_full_name, cast(created_at as date)

),

all_activity_dates as (

    select repo_id, activity_date from daily_commits
    union
    select repo_id, activity_date from daily_prs

)

select
    d.repo_id,
    d.activity_date,

    -- FK to dim_date: YYYYMMDD
    cast(strftime(d.activity_date, '%Y%m%d') as integer) as date_id,

    -- Commit metrics
    coalesce(c.commits_count, 0) as commits_count,
    coalesce(c.unique_committers, 0) as unique_committers,

    -- PR metrics
    coalesce(p.prs_opened, 0) as prs_opened,
    coalesce(p.prs_merged, 0) as prs_merged,
    p.avg_pr_close_hours

from all_activity_dates d

left join daily_commits c
    on d.repo_id = c.repo_id
   and d.activity_date = c.activity_date

left join daily_prs p
    on d.repo_id = p.repo_id
   and d.activity_date = p.activity_date