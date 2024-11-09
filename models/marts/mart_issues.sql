select
    concat('[', repo_name, ']', '(https://www.github.com/mrpowers-io/', repo_name, ')') as "repo",
    concat('[', user_name, ']', '(https://www.github.com/', user_name, ')')             as "author",
    concat('[', a.issue_title, ']',
           '(https://github.com/mrpowers-io/', c.repo_name,
           '/issues/', a.issue_number, ')'
    )                                                                                   as "title",
    coalesce(a.issue_body, '')                                                          as "body",
    strftime(a.created_at, '%Y-%m-%d')                                                  as "date",
    date_part('day', current_date - a.created_at)                                       as "days old",
    a.comments_count                                                                    as "comments"
from issues a
join users b
     on a.author_id = b.user_id
join repos c
     on a.repo_id = c.repo_id
where a.state = 'open'