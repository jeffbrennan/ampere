with
    closed_issues_past_month as (
        select
            repo_id,
            count(issue_id) as closed_issues
        from issues
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
        from issues
        where
            state = 'open'
    ),
    open_issues as (
        select
            repo_id,
            count(issue_id) as open_issues_count,
            avg(age_days)   as avg_age_days
        from open_issues_base
        group by repo_id
    ),
    new_issues as (
        select
            repo_id,
            count(issue_id) as new_issues_count,
        from issues
        where
            state = 'open'
            and created_at >= current_date - 120
        group by repo_id
    ),
    repo_spine as (
        select
            repo_id,
            repo_name
        from repos
    )
select
    concat('[', a.repo_name, ']', '(https://www.github.com/mrpowers-io/', a.repo_name, ')') as "repo",
    coalesce(b.open_issues_count, 0)                                                        as "open issues",
    ceil(coalesce(b.avg_age_days, 0))                                                       as "avg issue age (days)",
    coalesce(d.new_issues_count, 0)                                                         as "new issues (this month)",
    coalesce(c.closed_issues, 0)                                                            as "closed issues (this month)",
from repo_spine a
left join open_issues b
    on a.repo_id = b.repo_id
left join closed_issues_past_month c
    on a.repo_id = c.repo_id
left join new_issues d
    on a.repo_id = d.repo_id