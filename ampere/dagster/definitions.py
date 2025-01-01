from dagster import Definitions
from dagster_dbt import DbtCliResource

from .assets import (
    ampere_dbt_assets,
    dagster_get_commits,
    dagster_get_followers,
    dagster_get_following,
    dagster_get_forks,
    dagster_get_issues,
    dagster_get_pull_requests,
    dagster_get_pypi_downloads,
    dagster_get_releases,
    dagster_get_repos,
    dagster_get_stargazers,
    dagster_get_users,
    dagster_refresh_follower_network,
    dagster_refresh_star_network,
    dagster_test_run_fail,
)
from .project import ampere_project
from .schedules import schedules
from .sensors import email_on_run_failure

defs = Definitions(
    assets=[
        ampere_dbt_assets,
        dagster_get_stargazers,
        dagster_get_repos,
        dagster_get_releases,
        dagster_get_forks,
        dagster_get_issues,
        dagster_get_pull_requests,
        dagster_get_users,
        dagster_get_commits,
        dagster_get_followers,
        dagster_get_following,
        dagster_get_pypi_downloads,
        dagster_refresh_star_network,
        dagster_refresh_follower_network,
        dagster_test_run_fail,
    ],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=ampere_project),
    },
    sensors=[email_on_run_failure],
)
