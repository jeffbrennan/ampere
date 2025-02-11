with
unknown_python as (
    select
        repo,
        download_timestamp,
        group_name,
        group_value,
        download_count
    from {{ ref('int_downloads_melted_weekly') }}
    where
        group_name = 'python_version'
        and group_value = 'unknown'
),

python_major_minor_raw as (
    select
        repo,
        download_timestamp,
        group_name,
        concat(
            split_part(group_value, '.', 1),
            '.',
            split_part(group_value, '.', 2)
        ) as group_value,
        sum(download_count) as download_count
    from {{ ref('int_downloads_melted_weekly') }}
    where
        group_name = 'python_version'
        and group_value != 'unknown'
    group by all
),

recent_python_major_minor_counts as (
    select
        repo,
        group_name,
        group_value,
        sum(download_count) as download_count
    from python_major_minor_raw
    where
        download_timestamp
        >= (
            select max(b.download_timestamp) - interval 60 day
            from python_major_minor_raw as b
        )
    group by all
),

python_major_minor_ranked as (
    select
        repo,
        group_name,
        group_value,
        row_number() over (partition by repo, group_name
        order by download_count desc) as rn
    from recent_python_major_minor_counts
),

python_major_minor as (
    select
        a.repo,
        a.download_timestamp,
        a.group_name,
        case when b.group_value is null then 'other' else a.group_value end
            as group_value,
        sum(a.download_count) as download_count
    from python_major_minor_raw as a
    left join python_major_minor_ranked as b
    on a.repo = b.repo and a.group_name = b.group_name and a.group_value = b.group_value
    and b.rn <= 8
    group by all
),


package_versions_major_minor_raw as (
    select
        repo,
        download_timestamp,
        group_name,
        concat(
            split_part(group_value, '.', 1),
            '.',
            split_part(group_value, '.', 2)
        ) as group_value,
        sum(download_count) as download_count
    from {{ ref('int_downloads_melted_weekly') }}
    where
        group_name = 'package_version'
    group by all
),

recent_package_version_counts as (
select
    repo,
    group_name,
    group_value,
    sum(download_count) as download_count
    from package_versions_major_minor_raw
    where
        download_timestamp
        >= (
            select max(b.download_timestamp) - interval 60 day
            from package_versions_major_minor_raw as b
        )
    group by all
),

recent_package_versions_ranked as (
select
    repo,
    group_name,
    group_value,
    row_number() over (partition by repo, group_name
    order by download_count desc) as rn
    from recent_package_version_counts
),

package_versions as  (
    select
        a.repo,
        a.download_timestamp,
        a.group_name,
        case when b.group_value is null then 'other' else a.group_value end
            as group_value,
        sum(a.download_count) as download_count
    from package_versions_major_minor_raw as a
    left join recent_package_versions_ranked as b
        on a.repo = b.repo
        and a.group_name = b.group_name
        and a.group_value = b.group_value
        and b.rn <= 8
    group by all
),

operating_systems as (
    select
        repo,
        download_timestamp,
        group_name,
        group_value,
        download_count
    from {{ ref('int_downloads_melted_weekly') }}
    where group_name = 'system_name'
),

overall as (
    select
        repo,
        download_timestamp,
        group_name,
        group_value,
        sum(download_count) as download_count
    from {{ ref('int_downloads_melted_weekly') }}
    where group_name = 'overall'
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
    download_timestamp as download_date,
    group_name,
    group_value,
    download_count::uinteger as download_count
from combined
