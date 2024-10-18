with starred_repo_counts as (
select distinct
user_id,
len(repo_name_list) as starred_count
from {{ref('int_network_stargazers')}}
)
select
concat('[', b.user_name, ']', '(https://www.github.com/', b.user_name, ')')  user_name,
b.full_name as name,
b.followers_count as followers,
coalesce(c.starred_count, 0) as "org stars",
{{ dbt_utils.star(from=ref('int_network_stargazers_pivoted'), except=['user_id']) }}
from {{ ref('int_network_stargazers_pivoted') }} a
join users b
on a.user_id = b.user_id
join starred_repo_counts c
on a.user_id = c.user_id
