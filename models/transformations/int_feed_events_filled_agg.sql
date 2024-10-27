with
repo_list as (
    select distinct repo_id
    from {{ ref("int_repo_metrics") }}
    where repo_id is not null
),
event_list as (
    select distinct event_type
    from {{ ref("int_feed_events") }}
),
action_list as (
    select distinct event_action
    from {{ ref("int_feed_events") }}
    where event_action is not null
),
date_spine_full as (
    select distinct
        spine_date as event_date,
        repo_id,
        event_type,
        event_action
    from {{ ref("helper_date_spine") }}
    cross join repo_list
    cross join event_list
    cross join action_list
),
counts as (
select
    time_bucket('7 day', event_timestamp) as event_date,
    repo_id,
    event_type,
    event_action,
    count(event_id) as event_count
from {{ ref("int_feed_events") }}
group by all
)
select
    a.event_date,
    a.repo_id,
    a.event_type,
    a.event_action,
    coalesce(event_count, 0) as event_count
from date_spine_full a
left join counts b
    on a.event_date = b.event_date
    and a.repo_id = b.repo_id
    and a.event_type = b.event_type
    and a.event_action = b.event_action
