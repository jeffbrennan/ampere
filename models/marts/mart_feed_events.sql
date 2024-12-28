with base as (
    select distinct
        b.repo_name,
        c.user_name,
        c.full_name,
        a.event_id,
        a.event_type,
        a.event_action,
        a.event_data,
        a.event_timestamp
    from {{ ref("int_feed_events") }} as a
    inner join {{ ref("stg_repos") }} as b
        on a.repo_id = b.repo_id
    inner join  {{ ref("stg_users") }} as c
        on a.user_id = c.user_id
),

pr_numbers as (
    select distinct
        a.event_id,
        a.event_type,
        b.pr_number
    from base as a
    left join {{ ref("stg_pull_requests") }} as b
        on a.event_id = b.pr_id
    where a.event_type = 'pull request'
),

issue_numbers as (
    select distinct
        a.event_id,
        a.event_type,
        b.issue_number
    from base as a
    left join {{ ref("stg_issues") }} as b
        on a.event_id = b.issue_id
    where a.event_type = 'issue'
)

select
    a.*,
    case
        when
            a.event_type = 'pull request'
            then
                concat(
                    'https://github.com/mrpowers-io/',
                    a.repo_name,
                    '/pull/',
                    b.pr_number
                )
        when
            a.event_type = 'issue'
            then
                concat(
                    'https://github.com/mrpowers-io/',
                    a.repo_name,
                    '/issues/',
                    c.issue_number
                )
        when
            a.event_type = 'commit'
            then
                concat(
                    'https://github.com/mrpowers-io/',
                    a.repo_name,
                    '/commit/',
                    a.event_id
                )
        when
            a.event_type = 'fork'
            then concat('https://github.com/', a.user_name, '/', a.repo_name)
    end as event_link
from base as a
left join pr_numbers as b
    on
        a.event_id = b.event_id
        and a.event_type = b.event_type
left join issue_numbers as c
    on
        a.event_id = c.event_id
        and a.event_type = c.event_type
