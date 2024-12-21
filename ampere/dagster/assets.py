from typing import Any

from dagster import AssetExecutionContext, asset
from dagster_dbt import DbtCliResource, dbt_assets

from ampere.common import DeltaWriteConfig, get_model_primary_key, write_delta_table
from ampere.get_pypi_downloads import (
    refresh_all_pypi_downloads,
)
from ampere.get_repo_metrics import (
    get_commits,
    get_forks,
    get_issues,
    get_pull_requests,
    get_releases,
    get_repos,
    get_stargazers,
    get_user_ids,
    get_watchers,
    read_repos,
    refresh_followers,
    refresh_github_table,
    refresh_users,
)
from ampere.models import (
    Commit,
    Follower,
    Fork,
    Issue,
    PullRequest,
    Release,
    Stargazer,
    User,
    Watcher,
)

from .project import ampere_project

db_path = ampere_project.project_dir.joinpath("data/ampere.duckdb")


@dbt_assets(manifest=ampere_project.manifest_path)
def ampere_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource) -> Any:
    yield from dbt.cli(["build"], context=context).stream()


@asset(
    compute_kind="python",
    key=["repos"],
    group_name="github_metrics_daily_4",
)
def dagster_get_repos() -> None:
    repos = get_repos("mrpowers-io")
    write_delta_table(repos, "bronze", "repos", ["repo_id"])


@asset(
    compute_kind="python",
    key=["stargazers"],
    deps=["repos"],
    group_name="github_metrics_daily_4",
)
def dagster_get_stargazers(context: AssetExecutionContext) -> None:
    repos = read_repos()
    owner_name = "mrpowers-io"
    n = refresh_github_table(
        owner_name,
        repos,
        DeltaWriteConfig(
            table_dir="bronze",
            table_name=Stargazer.__tablename__,  # pyright: ignore [reportArgumentType]
            pks=get_model_primary_key(Stargazer),
        ),
        get_stargazers,
    )
    context.add_output_metadata({"n_records": n})


@asset(
    compute_kind="python",
    key=["forks"],
    deps=["stargazers"],
    group_name="github_metrics_daily_4",
)
def dagster_get_forks(context: AssetExecutionContext) -> None:
    repos = read_repos()
    owner_name = "mrpowers-io"
    n = refresh_github_table(
        owner_name,
        repos,
        DeltaWriteConfig(
            table_dir="bronze",
            table_name=Fork.__tablename__,  # pyright: ignore [reportArgumentType]
            pks=get_model_primary_key(Fork),
        ),
        get_forks,
    )
    context.add_output_metadata({"n_records": n})


@asset(
    compute_kind="python",
    key=["releases"],
    deps=["forks"],
    group_name="github_metrics_daily_4",
)
def dagster_get_releases(context: AssetExecutionContext) -> None:
    repos = read_repos()
    owner_name = "mrpowers-io"
    n = refresh_github_table(
        owner_name,
        repos,
        DeltaWriteConfig(
            table_dir="bronze",
            table_name=Release.__tablename__,  # pyright: ignore [reportArgumentType]
            pks=get_model_primary_key(Release),
        ),
        get_releases,
    )
    context.add_output_metadata({"n_records": n})


@asset(
    compute_kind="python",
    key=["pull_requests"],
    deps=["releases"],
    group_name="github_metrics_daily_4",
)
def dagster_get_pull_requests(context: AssetExecutionContext) -> None:
    repos = read_repos()
    owner_name = "mrpowers-io"
    n = refresh_github_table(
        owner_name,
        repos,
        DeltaWriteConfig(
            table_dir="bronze",
            table_name=PullRequest.__tablename__,  # pyright: ignore [reportArgumentType]
            pks=get_model_primary_key(PullRequest),
        ),
        get_pull_requests,
    )
    context.add_output_metadata({"n_records": n})


@asset(
    compute_kind="python",
    key=["issues"],
    deps=["pull_requests"],
    group_name="github_metrics_daily_4",
)
def dagster_get_issues(context: AssetExecutionContext) -> None:
    repos = read_repos()
    owner_name = "mrpowers-io"
    n = refresh_github_table(
        owner_name,
        repos,
        DeltaWriteConfig(
            table_dir="bronze",
            table_name=Issue.__tablename__,  # pyright: ignore [reportArgumentType]
            pks=get_model_primary_key(Issue),
        ),
        get_issues,
    )
    context.add_output_metadata({"n_records": n})


@asset(
    compute_kind="python",
    key=["watchers"],
    deps=["issues"],
    group_name="github_metrics_daily_4",
)
def dagster_get_watchers(context: AssetExecutionContext) -> None:
    repos = read_repos()
    owner_name = "mrpowers-io"
    n = refresh_github_table(
        owner_name,
        repos,
        DeltaWriteConfig(
            table_dir="bronze",
            table_name=Watcher.__tablename__,  # pyright: ignore [reportArgumentType]
            pks=get_model_primary_key(Watcher),
        ),
        get_watchers,
    )
    context.add_output_metadata({"n_records": n})


@asset(
    compute_kind="python",
    key=["commits"],
    deps=["watchers"],
    group_name="github_metrics_daily_4",
)
def dagster_get_commits(context: AssetExecutionContext) -> None:
    repos = read_repos()
    owner_name = "mrpowers-io"
    n = refresh_github_table(
        owner_name,
        repos,
        DeltaWriteConfig(
            table_dir="bronze",
            table_name=Commit.__tablename__,  # pyright: ignore [reportArgumentType]
            pks=get_model_primary_key(Commit),
        ),
        get_commits,
    )
    context.add_output_metadata({"n_records": n})


@asset(
    compute_kind="python",
    key=["users"],
    deps=["commits"],
    group_name="github_metrics_daily_4",
)
def dagster_get_users(context: AssetExecutionContext) -> None:
    user_ids = get_user_ids()
    n = refresh_users(
        user_ids,
        DeltaWriteConfig(
            table_dir="bronze",
            table_name=User.__tablename__,  # pyright: ignore [reportArgumentType]
            pks=get_model_primary_key(User),
        ),
    )

    context.add_output_metadata({"n_records": n})


@asset(
    compute_kind="python",
    key=["followers"],
    deps=["users"],
    group_name="github_followers_daily",
)
def dagster_get_followers(context: AssetExecutionContext) -> None:
    user_ids = get_user_ids()
    n = refresh_followers(
        user_ids,
        DeltaWriteConfig(
            table_dir="bronze",
            table_name=Follower.__tablename__,  # pyright: ignore [reportArgumentType]
            pks=get_model_primary_key(Follower),
        ),
    )

    context.add_output_metadata({"n_records": n})


@asset(compute_kind="python", key=["pypi_downloads"], group_name="bigquery_daily")
def dagster_get_pypi_downloads(context: AssetExecutionContext) -> None:
    records_added = refresh_all_pypi_downloads(dry_run=False)
    context.add_output_metadata({"n_records": records_added})
