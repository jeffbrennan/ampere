select
    b.repo_name,
    a.metric_type,
    a.metric_date,
    a.metric_count::bigint as metric_count
from
    {{ ref("int_repo_metrics_filled_partial") }} as a
left join {{ref("stg_repos")}} as b
    on a.repo_id = b.repo_id
left join {{ref("stg_users")}} as c
    on a.user_id = c.user_id
where a.metric_type in ('stars', 'issues', 'commits')
and a.metric_count is not null