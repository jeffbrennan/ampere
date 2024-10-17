select user_id, follower_id, max(retrieved_at) as retrieved_at
from followers
group by user_id, follower_id
