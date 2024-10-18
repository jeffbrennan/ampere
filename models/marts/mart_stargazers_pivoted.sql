select
concat('[', b.user_name, ']', '(https://www.github.com/', b.user_name, ')')  user_name,
b.full_name as name,
b.followers_count as followers,
{{ dbt_utils.star(from=ref('int_network_stargazers_pivoted'), except=['user_id']) }}
from {{ ref('int_network_stargazers_pivoted') }} a
join users b
on a.user_id = b.user_id
