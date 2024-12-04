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
from {{ ref('stg_downloads') }}

{% if is_incremental() %}
    where
        timestamp
        > (select coalesce(max(timestamp), '1900-01-01') from {{ this }}) --noqa
{% endif %}
