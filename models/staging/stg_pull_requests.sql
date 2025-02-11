{{ config(materialized='table') }}
with base as (
    select 
        *,
        row_number() over (partition by repo_id, pr_id order by retrieved_at desc) as rn
    from {{ source('main', 'pull_requests') }}
    where
        retrieved_at
        >= (
            select max(retrieved_at) - interval 24 as hours
            from {{ source('main', 'pull_requests') }}
        )
)
select
    repo_id,
    pr_id,
    pr_number,
    pr_title,
    pr_state,
    pr_body,
    author_id,
    created_at,
    updated_at,
    closed_at,
    merged_at,
    retrieved_at
from base
where rn = 1
