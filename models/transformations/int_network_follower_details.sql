with
user_stats as (
    select
        user_id,
        user_name,
        full_name,
        followers_count,
        following_count
    from {{ ref("stg_users") }}
),

internal_followers as (
    select
        a.user_id,
        list(b.user_name) as followers
    from {{ ref("stg_followers") }} as a
    inner join {{ ref("stg_users") }} as b on a.follower_id = b.user_id
    group by a.user_id
),

internal_following as (
    select
        a.follower_id as user_id,
        list(b.user_name) as following --noqa
    from {{ ref("stg_followers") }} as a
    inner join {{ ref("stg_users") }} as b on a.user_id = b.user_id
    group by a.follower_id
),

combined as (
    select
        a.user_id,
        a.user_name,
        a.full_name,
        a.followers_count,
        a.following_count,
        b.followers,
        c.following,
        coalesce(len(b.followers), 0) as internal_followers_count,
        coalesce(len(c.following), 0) as internal_following_count
    from user_stats as a
    left join internal_followers as b on a.user_id = b.user_id
    left join internal_following as c on a.user_id = c.user_id
)

select
    *,
    case
        when followers_count = 0
            then 0
        else coalesce(internal_followers_count, 0) / followers_count
    end as internal_followers_pct,
    case
        when following_count = 0
            then 0
        else coalesce(internal_following_count, 0) / following_count
    end as internal_following_pct
from combined
