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


with
base as (
    select * from {{ ref('stg_pypi_downloads') }}
    {% if is_incremental() %}
        where
            timestamp
            > (
                select coalesce(max(download_timestamp), '1900-01-01') --noqa
                from {{ this }}
            )
    {% endif %}
),

melted as (
    select
        project,
        timestamp,
        'country_code' as group_name,
        country_code as group_value,
        download_count
    from base
    union all
    select
        project,
        timestamp,
        'package_version' as group_name,
        package_version as group_value,
        download_count
    from base
    union all
    select
        project,
        timestamp,
        'python_version' as group_name,
        python_version as group_value,
        download_count
    from base
    union all
    select
        project,
        timestamp,
        'system_distro_name' as group_name,
        system_distro_name as group_value,
        download_count
    from base
    union all
    select
        project,
        timestamp,
        'system_distro_version' as group_name,
        system_distro_version as group_value,
        download_count
    from base
    union all
    select
        project,
        timestamp,
        'system_name' as group_name,
        system_name as group_value,
        download_count
    from base
    union all
    select
        project,
        timestamp,
        'system_release' as group_name,
        system_release as group_value,
        download_count
    from base
),
summed as (
    select
        project as repo,
        timestamp as download_timestamp,
        group_name,
        group_value,
        sum(download_count)::uinteger as download_count
    from melted
    group by all
),
overall as (
    select
        repo,
        download_timestamp,
        'overall' as group_name,
        'overall' as group_value,
        sum(download_count)::uinteger as download_count
    from summed
    where group_name = 'system_name'
    group by all
)
select * from summed
union all
select * from overall
