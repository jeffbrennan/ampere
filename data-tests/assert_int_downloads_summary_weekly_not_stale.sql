with melted_dates as (
    select group_name, max(download_timestamp) as max_date
    from {{ref('int_downloads_melted')}}
    group by all
),
weekly_dates as (
    select group_name, max(download_date) as max_date
    from {{ref('int_downloads_melted_weekly')}}
    where group_name in (select distinct group_name from melted_dates)
    group by all
),
comparison as (
    select
    a.group_name,
    a.max_date as melted_max_date,
    b.max_date as weekly_max_date,
    date_part('day', melted_max_date - weekly_max_date) as days_diff
    from melted_dates as a
    inner join weekly_dates as b
    on a.group_name = b.group_name
)

select * from comparison
where days_diff > 7

