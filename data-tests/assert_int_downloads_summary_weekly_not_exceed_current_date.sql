with dates as (
    select
        group_name, 
        max(download_timestamp) as max_timestamp
    from {{ ref('int_downloads_melted_weekly') }}
    group by all
)
select
    group_name,
    max_timestamp,
    now() as current_timestamp,
    (epoch(max_timestamp) - epoch(current_timestamp)) / 3600 as diff_hours
from dates
where max_timestamp > now()
