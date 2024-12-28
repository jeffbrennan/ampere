{{ config(materialized='table') }}
with base as (
    select 
        *,
        row_number() over (partition by repo_id, commit_id order by retrieved_at desc) as rn
    from {{ source('main', 'commits') }}
    where retrieved_at >= (select max(retrieved_at) - interval 3 hours from {{source('main', 'commits')}})
)
select
    repo_id,
    commit_id,
    author_id::bigint as author_id,
    comment_count,
    message,
    stats,
    committed_at,
    retrieved_at
from base
where rn = 1 