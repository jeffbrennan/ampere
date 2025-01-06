{{ config(materialized='view') }}
select
  {{ dbt_utils.pivot(
      'page',
      dbt_utils.get_column_values(ref('int_status_summary'), 'page'),
      agg='max',
      then_value='stale_emoji',
      else_value='null'
  ) }}
from {{ ref('int_status_summary') }}
