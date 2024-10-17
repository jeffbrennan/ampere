select a.user_id, a.follower_id
from {{ ref("stg_followers") }} a
join users b on a.user_id = b.user_id
join users c on a.follower_id = c.user_id

