{{ config(materialized='table') }}
with base as (
    select 
        *,
        row_number()
            over (partition by repo_id, fork_id, owner_id order by retrieved_at desc)
            as rn
    from {{ source('main', 'forks') }}
    where
        retrieved_at
        >= (
            select max(b.retrieved_at) - interval 24 hours --noqa: AL02
            from {{ source('main', 'forks') }} as b
        )
)
select
    repo_id,
    fork_id,
    owner_id,
    created_at,
    retrieved_at
from base
where rn = 1
