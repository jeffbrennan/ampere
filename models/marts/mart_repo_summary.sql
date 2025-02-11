select
    b.repo_name,
    a.metric_type,
    a.metric_date,
    a.metric_count::usmallint as metric_count
from
    {{ ref("int_repo_metrics_filled") }} as a
left join {{ ref("stg_repos") }} as b
    on a.repo_id = b.repo_id
where a.metric_type in ('stars', 'issues', 'commits')
and a.metric_count is not null
and a.metric_count <> 0
