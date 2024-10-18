 with repos_agg as (
    select
    a.user_id,
    list(c.repo_name) as repo_name_list,
    from users a
    inner join stargazers b
    on a.user_id = b.user_id
    inner join repos c
    on b.repo_id = c.repo_id
    group by a.user_id
)
 SELECT
 DISTINCT
    a.user_id,
    a.user_name,
    a.full_name,
    a.followers_count,
    b.starred_at,
    b.retrieved_at,
    c.repo_name,
    d.repo_name_list,
 FROM users a
 INNER JOIN stargazers b
 ON a.user_id = b.user_id
 INNER JOIN repos c
 ON b.repo_id = c.repo_id
 inner join repos_agg d
 on a.user_id = d.user_id
 ORDER BY a.user_name
