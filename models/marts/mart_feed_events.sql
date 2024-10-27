select distinct
    b.repo_name,
    c.user_name,
    c.full_name,
    a.event_id,
    a.event_type,
    a.event_action,
    a.event_data,
    a.event_timestamp
from {{ ref("int_feed_events") }} a
join repos b
on a.repo_id = b.repo_id
join users c
on a.user_id = c.user_id
