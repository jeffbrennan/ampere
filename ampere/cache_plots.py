import pickle
import random
from pathlib import Path
from typing import Any

import networkx as nx
import pandas as pd
from plotly.graph_objs import Figure

from ampere.cli.common import CLIEnvironment
from ampere.common import get_backend_db_con, timeit
from ampere.models import Followers, StargazerNetworkRecord, get_repos_with_downloads
from ampere.styling import ScreenWidth
from ampere.viz import (
    get_downloads_data,
    get_summary_data,
    viz_downloads,
    viz_follower_network,
    viz_star_network,
    viz_summary,
)

SCREEN_WIDTHS = [
    ScreenWidth.xs,
    ScreenWidth.sm,
    ScreenWidth.md,
    ScreenWidth.lg,
    ScreenWidth.xl,
]

MODES = ["light", "dark"]


def dump_obj_to_pickle(pkl_name: str, obj: Any):
    out_dir = Path(__file__).parents[1] / "data" / "viz"
    out_dir.mkdir(exist_ok=True, parents=True)

    out_path = out_dir / f"{pkl_name}.pkl"
    with out_path.open("wb") as f:
        pickle.dump(obj, f)


def dump_fig_to_json(f_name: str, fig: Figure):
    out_dir = Path(__file__).parents[1] / "data" / "viz"
    out_dir.mkdir(exist_ok=True, parents=True)

    out_path = out_dir / f"{f_name}.json"
    serialized_fig = fig.to_json()
    if not isinstance(serialized_fig, str):
        raise TypeError()

    with out_path.open("w") as f:
        f.write(serialized_fig)


def create_follower_network() -> None:
    con = get_backend_db_con()
    followers = (
        con.sql("select user_id, follower_id from int_internal_followers")
        .to_df()
        .to_dict(orient="records")
    )
    followers = list(Followers(**record) for record in followers)  # type: ignore

    random.seed(42)
    added_nodes = []
    graph = nx.Graph()
    for record in followers:
        if record.user_id not in added_nodes:
            graph.add_node(record.user_id)
            added_nodes.append(record.user_id)

        if record.follower_id not in added_nodes:
            graph.add_node(record.follower_id)
            added_nodes.append(record.follower_id)

        graph.add_edge(record.user_id, record.follower_id, weight=0.03)
    pos = nx.spring_layout(graph)
    nx.set_node_attributes(graph, pos, "pos")

    dump_obj_to_pickle("follower_network", graph)


@timeit
def cache_summary_plots() -> None:
    df = pd.DataFrame(get_summary_data())
    metrics = ["stars", "issues", "commits"]

    for metric in metrics:
        for mode in MODES:
            dark_mode = mode == "dark"
            for width in SCREEN_WIDTHS:
                fig = viz_summary(
                    df,
                    metric_type=metric,
                    date_range=None,
                    screen_width=width,
                    dark_mode=dark_mode,
                )

                f_name = f"summary_{metric}_{mode}_{width.value}"
                dump_obj_to_pickle(f_name, fig)


@timeit
def cache_downloads_plots() -> None:
    repos = get_repos_with_downloads(CLIEnvironment.dev)
    groups = ["overall", "package_version", "python_version"]
    for repo in repos:
        df = get_downloads_data(repo)
        dump_obj_to_pickle(f"downloads_df_{repo}", df)
        for group in groups:
            for mode in MODES:
                for width in SCREEN_WIDTHS:
                    dark_mode = mode == "dark"
                    pkl_name = f"downloads_{repo}_{group}_{mode}_{width}"
                    print(f"caching {pkl_name}...")
                    fig = viz_downloads(
                        df,
                        group_name=group,
                        date_range=None,
                        dark_mode=dark_mode,
                        screen_width=width,
                    )
                    dump_obj_to_pickle(pkl_name, fig)


def cache_stargazer_network():
    for mode in MODES:
        for width in SCREEN_WIDTHS:
            dark_mode = mode == "dark"
            fig = viz_star_network(dark_mode, width)
            f_name = f"stargazer_network_{mode}_{width}"
            dump_obj_to_pickle(f_name, fig)


def cache_follower_network():
    for mode in MODES:
        for width in SCREEN_WIDTHS:
            dark_mode = mode == "dark"
            fig = viz_follower_network(dark_mode, width)
            f_name = f"follower_network_{mode}_{width}"
            dump_obj_to_pickle(f_name, fig)


@timeit
def create_stargazer_network() -> None:
    con = get_backend_db_con()
    stargazers_df = con.sql(
        """
        select
            user_name,
            followers_count,
            starred_at,
            retrieved_at,
            repo_name
        from int_network_stargazers
    """
    ).to_df()

    stargazers = list(StargazerNetworkRecord(*record) for record in stargazers_df.values)
    repos_with_stargazers = list(set(i.repo_name for i in stargazers))

    print("creating star network...")
    random.seed(42)
    added_repos = []
    current_user = stargazers[0].user_name

    graph = nx.Graph()
    for repo in repos_with_stargazers:
        graph.add_node(repo, node_type="repo", repo=repo)

    for record in stargazers:
        if record.user_name != current_user:
            added_repos = []
        node_name = f"{record.user_name}_{record.repo_name}"
        graph.add_node(
            node_name,
            followers_count=record.followers_count,
            node_type="user_repo",
            repo=record.repo_name,
        )
        graph.add_edge(node_name, record.repo_name, weight=50, edge_type="user_repo")
        if len(added_repos) > 0:
            for added_repo in added_repos:
                graph.add_edge(
                    node_name,
                    f"{record.user_name}_{added_repo}",
                    edge_type="user_user",
                    weight=0.1,
                )
        added_repos.append(record.repo_name)
        current_user = record.user_name

    pos = nx.spring_layout(graph)
    nx.set_node_attributes(graph, pos, "pos")

    dump_obj_to_pickle("star_network", graph)


def refresh_all_caches():
    create_follower_network()
    cache_follower_network()

    create_stargazer_network()
    cache_stargazer_network()

    cache_summary_plots()
    cache_downloads_plots()


if __name__ == "__main__":
    refresh_all_caches()
