{{ config(materialized='table') }}

with recent_activity as (

    select
        repo_id,
        sum(commits_count) as recent_commits,
        sum(prs_merged) as recent_merged_prs,
        sum(unique_committers) as recent_contributors,
        avg(avg_pr_close_hours) as avg_pr_close_hours
    from {{ ref('fact_repo_activity') }}
    where activity_date >= current_date - interval '30 day'
    group by repo_id

),

community_history as (

    select
        repo_id,
        sum(prs_opened) as total_prs,
        sum(prs_merged) as merged_prs
    from {{ ref('fact_repo_activity') }}
    group by repo_id

),

base_metrics as (

    select
        r.repo_id,
        r.stars_count as stargazers_count,
        r.forks_count,
        r.watchers_count,

        coalesce(a.recent_commits, 0) as recent_commits,
        coalesce(a.recent_merged_prs, 0) as recent_merged_prs,
        coalesce(a.recent_contributors, 0) as recent_contributors,
        a.avg_pr_close_hours,

        coalesce(c.total_prs, 0) as total_prs,
        coalesce(c.merged_prs, 0) as merged_prs,

        case when coalesce(c.total_prs, 0) > 0
            then c.merged_prs * 1.0 / c.total_prs
            else 0
        end as pr_merge_ratio

    from {{ ref('dim_repository') }} r
    left join recent_activity a using (repo_id)
    left join community_history c using (repo_id)

),

ranked as (

    select
        *,

        ntile(10) over(order by stargazers_count desc) as rank_stars,
        ntile(10) over(order by forks_count desc) as rank_forks,
        ntile(10) over(order by watchers_count desc) as rank_watchers,

        ntile(10) over(order by recent_commits desc) as rank_commits,
        ntile(10) over(order by recent_contributors desc) as rank_contributors,

        -- SPEC: for reaction times where lower = better, use DESC in NTILE
        ntile(10) over(order by avg_pr_close_hours desc nulls last) as rank_pr_speed,

        ntile(10) over(order by pr_merge_ratio desc) as rank_pr_ratio

    from base_metrics

),

scored as (

    select
        repo_id,

        (rank_stars + rank_forks + rank_watchers) * 100.0 / 30 as score_popularity,
        (rank_commits + rank_contributors) * 100.0 / 20 as score_activity,
        rank_pr_speed * 100.0 / 10 as score_responsiveness,
        rank_pr_ratio * 100.0 / 10 as score_community

    from ranked

)

select
    repo_id,

    score_popularity,
    score_activity,
    score_responsiveness,
    score_community,

    (
        score_popularity * 0.2 +
        score_activity * 0.3 +
        score_responsiveness * 0.3 +
        score_community * 0.2
    ) as score_global,

    rank() over(order by
        (
            score_popularity * 0.2 +
            score_activity * 0.3 +
            score_responsiveness * 0.3 +
            score_community * 0.2
        ) desc
    ) as repo_rank

from scored
order by score_global desc
limit 10