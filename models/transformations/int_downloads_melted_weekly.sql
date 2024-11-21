with
    downloads as (
        select *
        from {{ ref('int_downloads_melted') }}

    )
select
    repo,
    time_bucket('7 day', download_timestamp) as download_date,
    group_name,
    group_value,
    sum(download_count)                      as download_count
from downloads
group by all
