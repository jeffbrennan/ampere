import pickle
import random
from pathlib import Path

import networkx as nx

from ampere.common import get_backend_db_con, timeit
from ampere.models import Followers, StargazerNetworkRecord


def dump_graph_to_pickle(pkl_name: str, graph: nx.Graph):
    out_dir = Path(__file__).parents[1] / "data" / "viz"
    out_dir.mkdir(exist_ok=True, parents=True)

    out_path = out_dir / f"{pkl_name}.pkl"
    with out_path.open("wb") as f:
        pickle.dump(graph, f)


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

    dump_graph_to_pickle("follower_network", graph)


@timeit
def create_star_network() -> None:
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

    dump_graph_to_pickle("star_network", graph)
