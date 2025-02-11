{{
    config(
        materialized='incremental',
        unique_key=[
          'project',
          'timestamp',
          'country_code',
          'package_version',
          'python_version',
          'system_distro_name', 
          'system_distro_version', 
          'system_name', 
          'system_release'
        ]
    )
}}

with downloads_numbered as (
select
    project,
    timestamp,
    country_code,
    package_version,
    python_version,
    system_distro_name,
    system_distro_version,
    system_name,
    system_release,
    download_count,
    retrieved_at,
    row_number() over (
        partition by 
            project,
            timestamp, 
            country_code, 
            package_version, 
            python_version, 
            system_distro_name, 
            system_distro_version, 
            system_name, 
            system_release
        order by retrieved_at desc
        ) as rn
    from {{ source('main', 'pypi_downloads') }}
    {% if is_incremental() %}
        where
            timestamp
            > (select coalesce(max(timestamp), '1900-01-01') from {{ this }}) --noqa
    {% endif %}
)
select
project,
    timestamp,
    country_code,
    package_version,
    python_version,
    system_distro_name,
    system_distro_version,
    system_name,
    system_release,
    download_count::bigint as download_count,
    retrieved_at
    from downloads_numbered
    where rn = 1
