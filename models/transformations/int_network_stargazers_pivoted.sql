with base as (
    select
        user_id,
        repo_name,
        strftime(starred_at, '%Y-%m-%d') as starred_at_date
    from {{ ref('int_network_stargazers') }}
)

select
    user_id,  --noqa: CV03
  {{ dbt_utils.pivot(
      'repo_name',
      dbt_utils.get_column_values(ref('int_network_stargazers'), 'repo_name'),
      agg='max',
      then_value='starred_at_date',
      else_value='null'
  ) }}
from base
group by user_id
