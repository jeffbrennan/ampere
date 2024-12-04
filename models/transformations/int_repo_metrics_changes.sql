-- only populate changes per repo and metric id
with
diffs as (
    select
        *,
        lag(metric_count) over (
            partition by repo_id, metric_type order by metric_date
        ) as prev_metric_count
    from {{ ref("int_repo_metrics_filled") }}

)

select --noqa
    repo_id,
    metric_type,
    metric_date,
    coalesce(metric_id, 'N/A') as metric_id,
    user_id::bigint as user_id,
    metric_count::bigint as metric_count
from diffs
where metric_count != prev_metric_count or metric_count is null
