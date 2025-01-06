{{ config(materialized='table') }}

with base as (
    select 
        *,
        row_number() over (partition by repo_id, user_id order by retrieved_at desc) as rn
    from {{ source('main', 'stargazers') }}
    where retrieved_at >= (select max(retrieved_at) - interval 24 hours from {{source('main', 'stargazers')}})
)
select
    repo_id,
    user_id,
    starred_at,
    retrieved_at
from base
where rn = 1
