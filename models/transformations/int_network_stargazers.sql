with repos_agg as (
    select
        a.user_id,
        list(c.repo_name) as repo_name_list
    from {{ ref('stg_users') }} as a
    inner join {{ ref('stg_stargazers') }} as b
        on a.user_id = b.user_id
    inner join {{ ref('stg_repos') }} as c
        on b.repo_id = c.repo_id
    group by a.user_id
)

select distinct
    a.user_id,
    a.user_name,
    a.full_name,
    a.followers_count,
    b.starred_at,
    b.retrieved_at,
    c.repo_name,
    d.repo_name_list
from {{ ref('stg_users') }} as a
inner join {{ ref('stg_stargazers') }}  as b
    on a.user_id = b.user_id
inner join {{ ref('stg_repos') }} as c
    on b.repo_id = c.repo_id
inner join repos_agg as d
    on a.user_id = d.user_id
order by a.user_name
