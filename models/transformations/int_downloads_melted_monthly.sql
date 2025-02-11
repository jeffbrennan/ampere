with
    date_spine_daily as (
        select distinct download_timestamp
        from {{ ref("int_downloads_melted_daily") }}
    ),
    date_spine_daily_numbered as (
        select
            download_timestamp,
            row_number() over (order by download_timestamp desc) - 1 as row_number
        from date_spine_daily
    ),
    date_spine_monthly as (
        select download_timestamp from date_spine_daily_numbered where row_number % 30 = 0
    ),
    downloads_rolling as (
        select
            repo,
            download_timestamp,
            group_name,
            group_value,
            sum(download_count) over (
                partition by repo, group_name, group_value
                order by
                    download_timestamp
                    range between interval '30' day preceding and current row --noqa: PRS
            ) as download_count
        from {{ ref("int_downloads_melted_daily") }}
    )
select
    a.download_timestamp,
    b.repo,
    b.group_name,
    b.group_value,
    b.download_count::uinteger as download_count
from date_spine_monthly as a
left join downloads_rolling as b on a.download_timestamp = b.download_timestamp
where b.download_timestamp is not null
