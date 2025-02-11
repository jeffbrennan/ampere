-- depends_on: {{ source('dagster', 'following') }}

{{ config(materialized='table') }}
with base as (
    select
        user_id,
        follower_id,
        retrieved_at,
        row_number()
            over (partition by user_id, follower_id order by retrieved_at desc)
            as rn
    from  {{ source('main', 'followers') }}
    where
        retrieved_at
        >= (
            select max(b.retrieved_at) - interval 24 hours --noqa: AL02
            from {{ source('main', 'followers') }} as b
        )
)
select
    user_id,
    follower_id,
    retrieved_at
from base
where rn = 1
