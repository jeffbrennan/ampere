select
    a.user_id,
    a.follower_id
from {{ ref("stg_followers") }} as a
inner join users as b on a.user_id = b.user_id
inner join users as c on a.follower_id = c.user_id
