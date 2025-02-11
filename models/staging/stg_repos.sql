{{ config(materialized='table') }}
with base as (
    select 
        *,
        row_number() over (partition by repo_id order by retrieved_at desc) as rn
    from {{ source('main', 'repos') }}
    where
        retrieved_at
        >= (
            select max(b.retrieved_at) - interval 24 hours --noqa: AL02
            from {{ source('main', 'repos') }} as b
        )
)
select
    repo_id,
    repo_name,
    license,
    topics,
    language,
    repo_size,
    forks_count,
    stargazers_count,
    open_issues_count,
    pushed_at,
    created_at,
    updated_at,
    retrieved_at
from base 
where rn = 1
