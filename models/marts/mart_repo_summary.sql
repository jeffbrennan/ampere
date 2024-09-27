select
	a.*,
	b.repo_name
from
	{{ ref('int_repo_metrics') }} a
	LEFT JOIN repos b
	on a.repo_id = b.repo_id