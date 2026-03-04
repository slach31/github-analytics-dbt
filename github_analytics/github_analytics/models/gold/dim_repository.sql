{{ config(materialized='table') }}

with repo as (
    select *
    from {{ ref('stg_repositories') }}
)

select
    repo_id,

    -- derive names from "owner/repo"
    split_part(repo_id, '/', 2) as repo_name,
    split_part(repo_id, '/', 1) as owner_login,

    description,
    language,
    license_name,

    stargazers_count as stars_count,
    forks_count      as forks_count,
    watchers_count   as watchers_count,

    created_at,
    repo_age_days,

    default_branch,
    has_wiki,
    has_pages

from repo