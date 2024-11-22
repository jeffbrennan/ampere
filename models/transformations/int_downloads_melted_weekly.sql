with
downloads as (
    select
        *,
        time_bucket('7 day', download_timestamp) as download_date
    from {{ ref('int_downloads_melted') }}

)

select
    repo,
    download_date,
    group_name,
    group_value,
    sum(download_count) as download_count
from downloads
where download_date < (select max(b.download_date) from downloads as b)
group by all
