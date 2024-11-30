{{
    config(
        materialized='incremental',
        unique_key=[
          'repo',
          'download_date',
          'group_name',
          'group_value'
        ]
    )
}}

with
downloads as (
    select
        *,
        time_bucket('7 day', download_timestamp) as download_date
    from {{ ref('int_downloads_melted') }}
    {% if is_incremental() %}
        where
            download_timestamp
            > (
                select coalesce(max(download_date), '1900-01-01') -- noqa
                from {{ this }}
            )
    {% endif %}


)

select
    repo,
    download_date,
    group_name,
    group_value,
    sum(download_count)::bigint as download_count
from downloads
--  exclude incomplete week
where download_date < (select max(b.download_date) from downloads as b)
group by all
