with
max_filled_vals as (
    select *
    from {{ ref("int_repo_metrics_filled") }}
    where
        metric_date
        = (
            select max(b.metric_date)
            from {{ ref("int_repo_metrics_filled") }} as b
        )
),

combined as (
    select *
    from {{ ref("int_repo_metrics_changes") }}
    where
        metric_date
        != (
            select max(b.metric_date)
            from {{ ref("int_repo_metrics_filled") }} as b
        )
    union distinct
    select *
    from max_filled_vals
)

select --noqa
    repo_id,
    metric_type,
    metric_date,
    coalesce(metric_id, 'N/A') as metric_id,
    user_id::bigint as user_id,
    metric_count::bigint as metric_count
from combined
