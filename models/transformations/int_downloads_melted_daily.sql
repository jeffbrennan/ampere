{{
    config(
        materialized='incremental',
        unique_key=[
          'repo',
          'download_timestamp',
          'group_name',
          'group_value'
        ]
    )
}}


with downloads_melted as (
    select 
        repo,
        download_timestamp,
        group_name,
        group_value,
        download_count
    from {{ ref('int_downloads_melted') }}
    {% if is_incremental() %}
        where
            download_timestamp
            >= (
                select coalesce(max(download_timestamp) + interval 1 day, date '1900-01-01') -- noqa
                from {{ this }}
            )
    {% endif %}

),

downloads_trunc as (
    select
        repo,
        time_bucket('1 day', download_timestamp) as download_timestamp,
        group_name,
        group_value,
        sum(download_count) as download_count
    from downloads_melted
    group by all
)

select
    repo,
    download_timestamp,
    group_name,
    group_value,
    sum(download_count)::uinteger as download_count
from downloads_trunc
group by all
