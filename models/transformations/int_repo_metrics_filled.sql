with
metric_list as (
    select distinct metric_type
    from {{ ref("int_repo_metrics") }}
    where metric_type is not null
),

repo_list as (
    select distinct repo_id
    from {{ ref("int_repo_metrics") }}
    where repo_id is not null
),

date_spine_full as (
    select distinct
        a.spine_date as metric_date,
        c.repo_id,
        b.metric_type
    from {{ ref("helper_date_spine") }} as a
    cross join metric_list as b
    cross join repo_list as c
),

metric_dates as (
    select
        repo_id,
        metric_type,
        time_bucket('7 day', metric_timestamp) as metric_date,
        max(metric_id) as metric_id,
        max(user_id) as user_id,
        max(metric_count) as metric_count
    from {{ ref("int_repo_metrics") }}
    group by all
),

metric_dates_complete as (
    select
        a.metric_date,
        a.repo_id,
        a.metric_type,
        b.metric_id,
        b.user_id,
        b.metric_count
    from date_spine_full as a
    left join
        metric_dates as b
        on
            a.metric_date = b.metric_date
            and a.metric_type = b.metric_type
            and a.repo_id = b.repo_id
),

metrics_fill_prep as (
    select
        metric_date,
        repo_id,
        metric_type,
        metric_id,
        user_id,
        metric_count,
        count(case when metric_id is not null then 1 end) over (
            partition by repo_id, metric_type
            order by metric_date
            rows between unbounded preceding and current row
        ) as metric_id_group,
        count(case when user_id is not null then 1 end) over (
            partition by repo_id, metric_type
            order by metric_date
            rows between unbounded preceding and current row
        ) as user_id_group,
        count(case when metric_count is not null then 1 end) over (
            partition by repo_id, metric_type
            order by metric_date
            rows between unbounded preceding and current row
        ) as metric_count_group
    from metric_dates_complete
),

metrics_filled_down as (
    select
        repo_id,
        metric_type,
        metric_date,
        min(metric_id) over (
            partition by repo_id, metric_type, metric_id_group
        ) as metric_id,
        min(user_id) over (
            partition by repo_id, metric_type, user_id_group
        ) as user_id,
        min(metric_count) over (
            partition by repo_id, metric_type, metric_count_group
        ) as metric_count
    from metrics_fill_prep
)

select *
from metrics_filled_down
