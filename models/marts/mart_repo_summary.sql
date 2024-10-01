select
	a.*,
	b.repo_name,
	c.user_name
from
	{{ ref("int_repo_metrics_filled") }} a
	left join repos b
	on a.repo_id = b.repo_id
	left join users c
	on a.user_id = c.user_id
