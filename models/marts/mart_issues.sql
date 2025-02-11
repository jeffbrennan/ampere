select
    a.comments_count as comments, --noqa
    concat(
        '[',
        c.repo_name,
        ']',
        '(https://www.github.com/mrpowers-io/',
        c.repo_name,
        ')'
    ) as repo,
    concat('[', b.user_name, ']', '(https://www.github.com/', b.user_name, ')')
        as author,
    concat(
        '[#', a.issue_number, ' ', a.issue_title, ']',
        '(https://github.com/mrpowers-io/', c.repo_name,
        '/issues/', a.issue_number, ')'
    ) as title,
    coalesce(a.issue_body, '') as body, --noqa
    strftime(a.created_at, '%Y-%m-%d') as date, --noqa
    date_part('day', current_date - a.created_at) as "days old" --noqa
from {{ ref('stg_issues') }} as a
inner join {{ ref('stg_users') }} as b
    on a.author_id = b.user_id
inner join {{ ref('stg_repos') }} as c
    on a.repo_id = c.repo_id
where a.state = 'open'
