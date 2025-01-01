select
    a.*,
    b.repo_name,
    c.user_name
from
    {{ ref("int_repo_metrics_filled_partial") }} as a
left join {{ref("stg_repos")}} as b
    on a.repo_id = b.repo_id
left join {{ref("stg_users")}} as c
    on a.user_id = c.user_id
