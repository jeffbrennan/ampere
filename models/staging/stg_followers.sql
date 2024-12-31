 -- depends_on: {{ source('dagster', 'following') }}

{{ config(materialized='table') }}
with base as (
    select
        user_id,
        follower_id,
        retrieved_at,
        row_number() over (partition by user_id, follower_id order by retrieved_at desc) as rn
    from  {{source('main', 'followers')}}
    where retrieved_at >= (select max(retrieved_at) - interval 8 hours from {{source('main', 'followers')}})
)
select
    user_id,
    follower_id,
    retrieved_at
from base
where rn = 1
