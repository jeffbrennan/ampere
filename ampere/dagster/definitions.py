from dagster import Definitions
from dagster_dbt import DbtCliResource

from .assets import (
    ampere_dbt_assets,
    dagster_get_commits,
    dagster_get_followers,
    dagster_get_forks,
    dagster_get_issues,
    dagster_get_pull_requests,
    dagster_get_releases,
    dagster_get_repos,
    dagster_get_stargazers,
    dagster_get_users,
    dagster_get_watchers,
    dagster_get_pypi_downloads,
)
from .project import ampere_project
from .schedules import schedules

defs = Definitions(
    assets=[
        ampere_dbt_assets,
        dagster_get_stargazers,
        dagster_get_repos,
        dagster_get_releases,
        dagster_get_forks,
        dagster_get_issues,
        dagster_get_pull_requests,
        dagster_get_watchers,
        dagster_get_users,
        dagster_get_commits,
        dagster_get_followers,
        dagster_get_pypi_downloads,
    ],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=ampere_project),
    },
)
