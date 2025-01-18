with dates as (
    select
        group_name, 
        min(download_date) as min_timestamp
    from int_downloads_melted_weekly
    group by all
)
select
    group_name,
    min_timestamp,
from dates
where min_timestamp >= now() - interval 365 days
