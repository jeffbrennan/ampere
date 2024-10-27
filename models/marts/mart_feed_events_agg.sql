select
    b.repo_name,
    a.event_type,
    a.event_action,
    a.event_count
from {{ ref("int_feed_events_filled_agg") }} a
join repos b
on a.repo_id = b.repo_id
