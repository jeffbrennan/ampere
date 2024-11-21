with
downloads as (
    select *
    from {{ ref('int_downloads_melted') }}

)

select
    repo,
    group_name,
    group_value,
    time_bucket('7 day', download_timestamp) as download_date,
    sum(download_count) as download_count
from downloads
group by all
