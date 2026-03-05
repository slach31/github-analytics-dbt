
select
    r.repo_id
from {{ ref('dim_repository') }} r
left join {{ ref('scoring_repositories') }} s
    on r.repo_id = s.repo_id
where s.repo_id is null