select
    repo_id,
    sum(prs_opened) as total_prs,
    sum(prs_merged) as merged_prs
from {{ ref('fact_repo_activity') }}
group by repo_id
having sum(prs_merged) > sum(prs_opened)