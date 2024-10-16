with
    max_filled_vals as (
        select *
        from {{ ref("int_repo_metrics_filled") }}
        where
            metric_date
            = (select max(metric_date) from {{ ref("int_repo_metrics_filled") }})
    ),
    combined as (
        select *
        from {{ ref("int_repo_metrics_changes") }}
        where metric_date != (select max(metric_date) from {{ ref("int_repo_metrics_filled") }})
        union
        select *
        from max_filled_vals
    )
select *
from combined
