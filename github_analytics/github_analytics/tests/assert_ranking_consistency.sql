-- tests/assert_ranking_consistency.sql
-- Fails if the #1 ranked repo does not have the best score

select
    repo_id,
    score_global,
    repo_rank
from {{ ref('scoring_repositories') }}
where repo_rank = 1
and score_global < (
    select max(score_global)
    from {{ ref('scoring_repositories') }}
)