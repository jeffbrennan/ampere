import time
from typing import Any

from dagster import AssetExecutionContext, asset
from dagster_dbt import DbtCliResource, dbt_assets

from ampere.cache_plots import (
    cache_downloads_plots,
    cache_follower_network,
    cache_stargazer_network,
    cache_summary_plots,
    create_follower_network,
    create_stargazer_network,
)
from ampere.common import (
    DeltaTableWriteMode,
    DeltaWriteConfig,
    get_backend_db_con,
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
from ampere.mirror import copy_backend_to_frontend
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

from .project import ampere_project

db_path = ampere_project.project_dir.joinpath("data/backend.duckdb")


# DBT
@dbt_assets(manifest=ampere_project.manifest_path)
def ampere_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource) -> Any:
    yield from dbt.cli(["build"], context=context).stream()


# GITHUB
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
    repos = read_repos(get_backend_db_con())
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
    repos = read_repos(get_backend_db_con())
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
    repos = read_repos(get_backend_db_con())
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
    repos = read_repos(get_backend_db_con())
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
    repos = read_repos(get_backend_db_con())
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
    repos = read_repos(get_backend_db_con())
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
    deps=["int_network_stargazers", "github_metrics_backend_to_frontend"],
    group_name="github_metrics_daily_4",
)
def dagster_refresh_star_network(context: AssetExecutionContext) -> None:
    start_time = time.time()
    create_stargazer_network()
    cache_stargazer_network()
    context.add_output_metadata({"elapsed time": time.time() - start_time})


@asset(
    compute_kind="python",
    key=["refresh_follower_network"],
    deps=[
        "int_network_follower_details",
        "github_metrics_backend_to_frontend",
        "refresh_star_network",
    ],
    group_name="github_metrics_daily_4",
)
def dagster_refresh_follower_network(context: AssetExecutionContext) -> None:
    start_time = time.time()
    create_follower_network()
    cache_follower_network()
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


@asset(
    compute_kind="python",
    key=["github_metrics_backend_to_frontend"],
    deps=[
        "stg_repos",
        "int_network_stargazers",
        "int_network_follower_details",
        "mart_feed_events",
        "mart_issues",
        "mart_issues_summary",
        "mart_stargazers_pivoted",
        "mart_repo_summary",
    ],
    group_name="github_metrics_daily_4",
)
def github_metrics_table_copy(context: AssetExecutionContext) -> None:
    start_time = time.time()
    copy_backend_to_frontend()
    context.add_output_metadata({"elapsed_time": time.time() - start_time})


@asset(
    compute_kind="python",
    key=["refresh_summary_plots"],
    deps=["github_metrics_backend_to_frontend", "mart_repo_summary"],
    group_name="github_metrics_daily_4",
)
def dagster_refresh_summary_plots(context: AssetExecutionContext) -> None:
    start_time = time.time()
    cache_summary_plots()
    context.add_output_metadata({"elapsed time": time.time() - start_time})


# BIGQUERY
@asset(compute_kind="python", key=["pypi_downloads"], group_name="bigquery_daily")
def dagster_get_pypi_downloads(context: AssetExecutionContext) -> None:
    records_added = refresh_all_pypi_downloads(dry_run=False)
    context.add_output_metadata({"n_records": records_added})


@asset(
    compute_kind="python",
    key=["bigquery_backend_to_frontend"],
    deps=["mart_downloads_summary"],
    group_name="bigquery_daily",
)
def bigquery_table_copy(context: AssetExecutionContext) -> None:
    start_time = time.time()
    copy_backend_to_frontend()
    context.add_output_metadata({"elapsed_time": time.time() - start_time})


@asset(
    compute_kind="python",
    key=["refresh_downloads_plots"],
    deps=["bigquery_backend_to_frontend", "mart_downloads_summary"],
    group_name="bigquery_daily",
)
def dagster_refresh_downloads_plots(context: AssetExecutionContext) -> None:
    start_time = time.time()
    cache_downloads_plots()
    context.add_output_metadata({"elapsed time": time.time() - start_time})


# TESTS
@asset(compute_kind="python", key=["will_pass"], group_name="test")
def dagster_test_run_pass() -> None:
    assert True


@asset(compute_kind="python", key=["will_fail1"], deps=["will_pass"], group_name="test")
def dagster_test_run_fail() -> None:
    raise AssertionError("failed on line 42: this asset always fails. asset1")


@asset(compute_kind="python", key=["will_fail2"], deps=["will_pass"], group_name="test")
def dagster_test_run_fail2() -> None:
    raise AssertionError("failed on line 42: this asset always fails. asset2")
