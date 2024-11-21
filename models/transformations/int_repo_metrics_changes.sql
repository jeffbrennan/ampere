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

select
    repo_id,
    metric_type,
    metric_date,
    metric_id,
    user_id,
    metric_count
from diffs
where metric_count != prev_metric_count or metric_count is null
