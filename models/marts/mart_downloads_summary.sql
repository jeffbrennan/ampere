select
    repo,
    download_date,
    group_name,
    group_value,
    download_count::uinteger as download_count
from {{ ref('int_downloads_summary') }}
where group_name <> 'system_name'
