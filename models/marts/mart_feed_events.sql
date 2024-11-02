with base as
(
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
),
pr_numbers as (
    select distinct event_id, event_type, pr_number
    from base a
    left join pull_requests b
        on a.event_id = b.pr_id
    where a.event_type == 'pull request'
),
issue_numbers as (
    select distinct event_id, event_type, issue_number
    from base a
    left join issues b
        on a.event_id = b.issue_id
    where a.event_type == 'issue'
)
select
    a.*,
    case
        when a.event_type == 'pull request' then concat('https://github.com/mrpowers-io/', a.repo_name, '/pull/', b.pr_number)
        when a.event_type == 'issue' then concat('https://github.com/mrpowers-io/', a.repo_name, '/issues/', c.issue_number)
        when a.event_type == 'commit' then concat('https://github.com/mrpowers-io/', a.repo_name, '/commit/', a.event_id)
        when a.event_type == 'fork' then concat('https://github.com/', a.user_name, '/', a.repo_name)
    end event_link
from base a
left join pr_numbers b
    on a.event_id = b.event_id
    and a.event_type = b.event_type
left join issue_numbers c
    on a.event_id = c.event_id
    and a.event_type = c.event_type
