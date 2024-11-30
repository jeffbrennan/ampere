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
    retrieved_at
from {{ source('main', 'pypi_downloads') }}
