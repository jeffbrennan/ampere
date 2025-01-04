{{
    config(
        materialized='incremental',
        unique_key=[
          'repo',
          'download_date',
          'group_name',
          'group_value'
        ],

    )
}}


with downloads_melted as (
    select 
        repo,
        download_timestamp,
        group_name,
        group_value,
        download_count
    from {{ref('int_downloads_melted')}}
    {% if is_incremental() %}
        where
            download_timestamp
            > (
                select coalesce(max(download_date) + interval 7 days, '1900-01-01') -- noqa
                from {{ this }}
            )
    {% endif %}
    
),

downloads_old as (
    select
        repo,
        time_bucket('70 day', download_timestamp) as download_date,
        group_name,
        group_value,
        floor(sum(download_count) / 10) as download_count
    from downloads_melted
    where download_timestamp < (select max(download_timestamp) - interval 730 days from downloads_melted)
    group by all
),

downloads_mid as (
    select
        repo,
        time_bucket('28 day', download_timestamp) as download_date,
        group_name,
        group_value,
        floor(sum(download_count) / 4) as download_count
    from downloads_melted
    where download_timestamp >= (select max(download_timestamp) - interval 730 days from downloads_melted)
    and download_timestamp < (select max(download_timestamp) - interval 365 days from downloads_melted)
    group by all
),

downloads_new as (
    select
        repo,
        time_bucket('7 day', download_timestamp) as download_date,
        group_name,
        group_value,
        sum(download_count) as download_count
    from downloads_melted
    where download_timestamp >= (select max(download_timestamp) - interval 365 days from downloads_melted)
    group by all  
),

downloads_trunc as (
    select * from downloads_old
    union all 
    select * from downloads_mid
    union all
    select * from downloads_new
)

select
    repo,
    download_date,
    group_name,
    group_value,
    sum(download_count)::bigint as download_count
from downloads_trunc 
where
    download_date
    < (select coalesce(max(b.download_date), '1900-01-01') from downloads_trunc as b)
group by all
