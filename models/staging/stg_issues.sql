{{ config(materialized='table') }}
with base as (
    select 
        *,
        row_number()
            over (partition by repo_id, issue_id order by retrieved_at desc)
            as rn
    from {{ source('main', 'issues') }}
    where
        retrieved_at
        >= (
            select max(b.retrieved_at) - interval 24 hours --noqa: AL02
            from {{ source('main', 'issues') }} as b
        )
)
select
    repo_id,
    issue_id,
    issue_number,
    issue_title,
    issue_body,
    author_id,
    state,
    state_reason,
    comments_count,
    created_at,
    updated_at,
    closed_at,
    retrieved_at
from base
where rn = 1
