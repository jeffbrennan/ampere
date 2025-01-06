{{ config(materialized='table') }}
with base as (
    select 
        *,
        row_number() over (partition by user_id order by retrieved_at desc) as rn
    from {{ source('main', 'users') }}
    where retrieved_at >= (select max(retrieved_at) - interval 24 hours from {{source('main', 'users')}})
)
select
    user_id,
    user_name,
    full_name,
    company,
    avatar_url,
    repos_count,
    followers_count,
    following_count,
    created_at,
    updated_at,
    retrieved_at
from base
where rn = 1
