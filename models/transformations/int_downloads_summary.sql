with
unknown_python as (
    select
        repo,
        download_date,
        group_name,
        group_value,
        download_count
    from {{ ref('int_downloads_melted_weekly') }}
    where
        repo not in ('pyspark', 'deltalake')
        and group_name = 'python_version'
        and group_value = 'unknown'
),

python_major_minor as (
    select
        repo,
        download_date,
        group_name,
        concat(
            split_part(group_value, '.', 1),
            '.',
            split_part(group_value, '.', 2)
        ) as group_value,
        sum(download_count) as download_count
    from {{ ref('int_downloads_melted_weekly') }}
    where
        repo not in ('pyspark', 'deltalake')
        and group_name = 'python_version'
        and group_value != 'unknown'
    group by all
),

clouds as (
    select
        repo,
        download_date,
        group_name,
        case
            when contains(group_value, 'amzn') then 'aws'
            when contains(group_value, 'gcp') then 'gcp'
            when contains(group_value, 'azure') then 'azure'
            when group_value = 'unknown' then 'unknown'
            else 'other'
        end as group_value,
        sum(download_count) as download_count
    from {{ ref('int_downloads_melted_weekly') }}
    where repo not in ('pyspark', 'deltalake') and group_name = 'system_release'
    group by all
),

package_versions as (
    select
        repo,
        download_date,
        group_name,
        group_value,
        download_count
    from {{ ref('int_downloads_melted_weekly') }}
    where
        repo not in ('pyspark', 'deltalake') and group_name = 'package_version'
),

operating_systems as (
    select
        repo,
        download_date,
        group_name,
        group_value,
        download_count
    from {{ ref('int_downloads_melted_weekly') }}
    where repo not in ('pyspark', 'deltalake') and group_name = 'system_name'
),

overall as (
    select
        repo,
        download_date,
        'overall' as group_name,
        'overall' as group_value,
        sum(download_count) as download_count
    from {{ ref('int_downloads_melted_weekly') }}
    where repo not in ('pyspark', 'deltalake') and group_name = 'system_name'
    group by all
),

combined as (
    select *
    from unknown_python
    union all
    select *
    from python_major_minor
    union all
    select *
    from clouds
    union all
    select *
    from package_versions
    union all
    select *
    from operating_systems
    union all
    select *
    from overall
)

select
    repo,
    download_date,
    group_name,
    group_value,
    download_count::uinteger as download_count
from combined
