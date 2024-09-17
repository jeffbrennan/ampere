from typing import Any

from dagster import AssetExecutionContext, asset
from dagster_dbt import DbtCliResource, dbt_assets

from ampere.common import write_delta_table, DeltaWriteConfig, get_model_primary_key
from ampere.get_repo_metrics import (
    get_repos,
    read_repos,
    refresh_github_table,
    get_stargazers,
    get_pull_requests,
    get_releases,
    get_forks,
    get_issues,
    get_watchers,
    get_user_ids,
    refresh_users,
    get_commits,
)
from ampere.models import (
    Stargazer,
    Fork,
    Release,
    PullRequest,
    Issue,
    Watcher,
    User,
    Commit,
)
from .project import ampere_project

db_path = ampere_project.project_dir.joinpath("data/ampere.duckdb")


@dbt_assets(manifest=ampere_project.manifest_path)
def ampere_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource) -> Any:
    yield from dbt.cli(["build"], context=context).stream()


@asset(compute_kind="python", key=["repos"])
def dagster_get_repos() -> None:
    repos = get_repos("mrpowers-io")
    write_delta_table(repos, "bronze", "repos", ["repo_id"])


@asset(compute_kind="python", key=["stargazers"], deps=["repos"])
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


@asset(compute_kind="python", key=["forks"], deps=["stargazers"])
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


@asset(compute_kind="python", key=["releases"], deps=["forks"])
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


@asset(compute_kind="python", key=["pull_requests"], deps=["releases"])
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


@asset(compute_kind="python", key=["issues"], deps=["pull_requests"])
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


@asset(compute_kind="python", key=["watchers"], deps=["issues"])
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


@asset(compute_kind="python", key=["commits"], deps=["watchers"])
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


@asset(compute_kind="python", key=["users"], deps=["commits"])
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
