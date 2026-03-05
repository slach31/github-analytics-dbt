select
    repo_full_name as repo_id,
    pr_number,
    created_at,
    closed_at,
    merged_at
from {{ ref('stg_pull_requests') }}
where (closed_at is not null and closed_at < created_at)
   or (merged_at is not null and merged_at < created_at)