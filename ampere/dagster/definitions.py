from dagster import Definitions
from dagster_dbt import DbtCliResource

from .assets import (
    ampere_dbt_assets,
    bigquery_table_copy,
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
    dagster_refresh_downloads_plots,
    dagster_refresh_follower_network,
    dagster_refresh_star_network,
    dagster_refresh_summary_plots,
    dagster_test_run_fail,
    dagster_test_run_fail2,
    dagster_test_run_pass,
    github_metrics_table_copy,
)
from .jobs import bigquery_daily_job, github_metrics_daily_4_job
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
        dagster_refresh_summary_plots,
        dagster_refresh_downloads_plots,
        github_metrics_table_copy,
        bigquery_table_copy,
        dagster_test_run_fail,
        dagster_test_run_fail2,
        dagster_test_run_pass,
    ],
    jobs=[github_metrics_daily_4_job, bigquery_daily_job],
    schedules=schedules,
    resources={
        "dbt": DbtCliResource(project_dir=ampere_project),
    },
    sensors=[email_on_run_failure],
)
