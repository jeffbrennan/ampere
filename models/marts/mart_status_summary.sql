select
    summary,
    downloads,
    feed,
    issues,
    network_stargazers,
    network_followers
from {{ ref('int_status_summary_pivoted') }}
