with melted_dates as (
    select group_name, max(download_timestamp) as max_date
    from {{ ref('int_downloads_melted') }}
    group by all
),
daily_dates as (
    select group_name, max(download_timestamp) as max_date
    from {{ ref('int_downloads_melted_daily') }}
    where group_name in (select distinct b.group_name from melted_dates as b)
    group by all
),
weekly_dates as (
    select group_name, max(download_timestamp) as max_date
    from {{ ref('int_downloads_melted_weekly') }}
    where group_name in (select distinct b.group_name from melted_dates as b)
    group by all
),
monthly_dates as (
    select group_name, max(download_timestamp) as max_date
    from {{ ref('int_downloads_melted_monthly') }}
    where group_name in (select distinct b.group_name from melted_dates as b)
    group by all
)

select
    a.group_name,
    a.max_date as hourly_max_date,
    b.max_date as daily_max_date,
    c.max_date as weekly_max_date,
    d.max_date as monthly_max_date,
    date_part('day', hourly_max_date - daily_max_date) as daily_days_diff,
    date_part('day', hourly_max_date - weekly_max_date) as weekly_days_diff,
    date_part('day', hourly_max_date - monthly_max_date) as monthly_days_diff
    from melted_dates as a
    inner join daily_dates as b
    on a.group_name = b.group_name
    inner join weekly_dates as c
    on a.group_name = c.group_name
    inner join monthly_dates as d
    on a.group_name = d.group_name
