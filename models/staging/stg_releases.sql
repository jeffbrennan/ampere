{{ config(materialized='table') }}
with base as (
    select 
        *,
        row_number()
            over (partition by repo_id, release_id order by retrieved_at desc)
            as rn
    from {{ source('main', 'releases') }}
    where
        retrieved_at
        >= (
            select max(b.retrieved_at) - interval 24 hours --noqa: AL02
            from {{ source('main', 'releases') }} as b
        )
)
select
    repo_id,
    release_id,
    release_name,
    tag_name,
    release_body,
    created_at,
    published_at,
    retrieved_at
from base
where rn = 1
