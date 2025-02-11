with
closed_issues_past_month as (
    select
        repo_id,
        count(issue_id) as closed_issues
    from {{ ref('stg_issues') }}
    where
        state = 'closed'
        and closed_at >= current_date - 120
    group by repo_id
),

open_issues_base as (
    select
        repo_id,
        issue_id,
        date_part('day', current_date - created_at) as age_days
    from {{ ref('stg_issues') }}
    where
        state = 'open'
),

open_issues as (
    select
        repo_id,
        count(issue_id) as open_issues_count,
        median(age_days) as median_age_days
    from open_issues_base
    group by repo_id
),

new_issues as (
    select
        repo_id,
        count(issue_id) as new_issues_count
    from {{ ref('stg_issues') }}
    where
        state = 'open'
        and created_at >= current_date - 120
    group by repo_id
),

repo_spine as (
    select
        repo_id,
        repo_name
    from {{ ref('stg_repos') }}
)

select
    concat(
        '[',
        a.repo_name,
        ']',
        '(https://www.github.com/mrpowers-io/',
        a.repo_name,
        ')'
    ) as repo,
    coalesce(b.open_issues_count, 0) as "open issues", --noqa
    ceil(coalesce(b.median_age_days, 0)) as "median issue age (days)", --noqa
    coalesce(d.new_issues_count, 0) as "new issues (this month)", --noqa
    coalesce(c.closed_issues, 0) as "closed issues (this month)" --noqa
from repo_spine as a
left join open_issues as b
    on a.repo_id = b.repo_id
left join closed_issues_past_month as c
    on a.repo_id = c.repo_id
left join new_issues as d
    on a.repo_id = d.repo_id
