
{{ config(materialized='table') }}
with base as (
    select 
        *,
        row_number() over (partition by repo_id, fork_id, owner_id order by retrieved_at desc) as rn
    from {{ source('main', 'forks') }}
    where retrieved_at >= (select max(retrieved_at) - interval 3 hours from {{source('main', 'forks')}})
)
select
    repo_id,
    fork_id,
    owner_id,
    created_at,
    retrieved_at
from base
where rn = 1
