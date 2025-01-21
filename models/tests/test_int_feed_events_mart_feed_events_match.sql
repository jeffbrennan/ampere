with int_timestamps as (
    select
        event_type,
        max(event_timestamp) as max_timestamp
    from {{ ref('int_feed_events') }}
    group by all
),
mart_timestamps as (
    select
        event_type,
        max(event_timestamp) as max_timestamp
    from {{ ref('mart_feed_events') }}
    group by all
)
select
    a.event_type,
    a.max_timestamp as max_int_timestamp,
    b.max_timestamp as max_mart_timestamp
from int_timestamps a
inner join mart_timestamps b
on a.event_type = b.event_type
where max_int_timestamp <> max_mart_timestamp
