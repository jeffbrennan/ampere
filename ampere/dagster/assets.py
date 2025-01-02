import time
from typing import Any

from dagster import AssetExecutionContext, asset
from dagster_dbt import DbtCliResource, dbt_assets

from ampere.common import (
    DeltaTableWriteMode,
    DeltaWriteConfig,
    divide_chunks,
    get_model_primary_key,
    write_delta_table,
)
from ampere.get_pypi_downloads import (
    refresh_all_pypi_downloads,
)
from ampere.get_repo_metrics import (
    get_commits,
    get_forks,
    get_issues,
    get_org_user_ids,
    get_pull_requests,
    get_releases,
    get_repos,
    get_stale_followers_user_ids,
    get_stargazers,
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
    Repo,
    Stargazer,
    User,
)
from ampere.viz import viz_follower_network, viz_star_network

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
def dagster_get_repos(context: AssetExecutionContext) -> None:
    repos = get_repos("mrpowers-io")
    write_delta_table(
        records=repos,
        config=DeltaWriteConfig(
            table_dir="bronze",
            table_name=str(Repo.__tablename__),
            pks=get_model_primary_key(Repo),
            mode=DeltaTableWriteMode.APPEND,
        ),
    )
    context.add_output_metadata({"n_records": len(repos)})


@asset(
    compute_kind="python",
    key=["stargazers"],
    deps=["stg_repos"],
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
            mode=DeltaTableWriteMode.APPEND,
        ),
        get_stargazers,
    )
    context.add_output_metadata({"n_records": n})


@asset(
    compute_kind="python",
    key=["forks"],
    deps=["stg_repos"],
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
            mode=DeltaTableWriteMode.APPEND,
        ),
        get_forks,
    )
    context.add_output_metadata({"n_records": n})


@asset(
    compute_kind="python",
    key=["releases"],
    deps=["stg_repos"],
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
            mode=DeltaTableWriteMode.APPEND,
        ),
        get_releases,
    )
    context.add_output_metadata({"n_records": n})


@asset(
    compute_kind="python",
    key=["pull_requests"],
    deps=["stg_repos"],
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
            mode=DeltaTableWriteMode.APPEND,
        ),
        get_pull_requests,
    )
    context.add_output_metadata({"n_records": n})


@asset(
    compute_kind="python",
    key=["issues"],
    deps=["stg_repos"],
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
            mode=DeltaTableWriteMode.APPEND,
        ),
        get_issues,
    )
    context.add_output_metadata({"n_records": n})


@asset(
    compute_kind="python",
    key=["commits"],
    deps=["stg_repos"],
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
            mode=DeltaTableWriteMode.APPEND,
        ),
        get_commits,
    )
    context.add_output_metadata({"n_records": n})


@asset(
    compute_kind="python",
    key=["users"],
    deps=[
        "stg_stargazers",
        "stg_forks",
        "stg_commits",
        "stg_issues",
        "stg_pull_requests",
    ],
    group_name="github_metrics_daily_4",
)
def dagster_get_users(context: AssetExecutionContext) -> None:
    user_ids = get_org_user_ids()
    n = refresh_users(
        user_ids,
        DeltaWriteConfig(
            table_dir="bronze",
            table_name=User.__tablename__,  # pyright: ignore [reportArgumentType]
            pks=get_model_primary_key(User),
            mode=DeltaTableWriteMode.APPEND,
        ),
    )

    context.add_output_metadata({"n_records": n})


@asset(
    compute_kind="python",
    key=["refresh_star_network"],
    deps=["int_network_stargazers"],
    group_name="github_metrics_daily_4",
)
def dagster_refresh_star_network(context: AssetExecutionContext) -> None:
    start_time = time.time()
    _ = viz_star_network(use_cache=False, show_fig=False)
    context.add_output_metadata({"elapsed time": time.time() - start_time})


@asset(
    compute_kind="python",
    key=["refresh_follower_network"],
    deps=["int_network_follower_details"],
    group_name="github_metrics_daily_4",
)
def dagster_refresh_follower_network(context: AssetExecutionContext) -> None:
    start_time = time.time()
    _ = viz_follower_network(use_cache=False, show_fig=False)
    context.add_output_metadata({"elapsed time": time.time() - start_time})


@asset(
    compute_kind="python",
    key=["followers"],
    deps=["stg_users"],
    group_name="github_metrics_daily_4",
)
def dagster_get_followers(context: AssetExecutionContext) -> None:
    user_ids = get_stale_followers_user_ids("followers")
    n = refresh_followers(
        user_ids,
        DeltaWriteConfig(
            table_dir="bronze",
            table_name=Follower.__tablename__,  # pyright: ignore [reportArgumentType]
            pks=get_model_primary_key(Follower),
            mode=DeltaTableWriteMode.APPEND,
        ),
        "followers",
    )

    context.add_output_metadata({"n_records": n})


@asset(
    compute_kind="python",
    key=["following"],
    deps=["stg_users"],
    group_name="github_metrics_daily_4",
)
def dagster_get_following(context: AssetExecutionContext) -> None:
    user_ids = get_stale_followers_user_ids("following")
    n = refresh_followers(
        user_ids,
        DeltaWriteConfig(
            table_dir="bronze",
            table_name=Follower.__tablename__,  # pyright: ignore [reportArgumentType]
            pks=get_model_primary_key(Follower),
            mode=DeltaTableWriteMode.APPEND,
        ),
        "following",
    )

    context.add_output_metadata({"n_records": n})


@asset(compute_kind="python", key=["pypi_downloads"], group_name="bigquery_daily")
def dagster_get_pypi_downloads(context: AssetExecutionContext) -> None:
    records_added = refresh_all_pypi_downloads(dry_run=False)
    context.add_output_metadata({"n_records": records_added})


@asset(compute_kind="python", key=["will_pass"], group_name="test")
def dagster_test_run_pass() -> None:
    assert True


@asset(compute_kind="python", key=["will_fail1"], deps=["will_pass"], group_name="test")
def dagster_test_run_fail() -> None:
    raise AssertionError("failed on line 42: this asset always fails. asset1")


@asset(compute_kind="python", key=["will_fail2"], deps=["will_pass"], group_name="test")
def dagster_test_run_fail2() -> None:
    raise AssertionError("failed on line 42: this asset always fails. asset2")
