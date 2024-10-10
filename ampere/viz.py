import datetime
import pickle
import random
import time
from dataclasses import dataclass
from functools import wraps
from pathlib import Path

import networkx as nx
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.graph_objs import Figure

from ampere.common import get_db_con, timeit


@dataclass(slots=True, frozen=True)
class StargazerNetworkRecord:
    user_name: str
    followers_count: int
    starred_at: datetime.datetime
    retrieved_at: datetime.datetime
    repo_name: str


@dataclass(slots=True, frozen=True)
class FollowerInfo:
    user_name: str
    followers_count: int
    internal_followers_count: int
    follower_name: str
    follower_followers_count: int
    follower_internal_followers_count: int


# intended for traces to appear in this order
REPO_PALETTE = {
    "spark-fast-tests": "#636EFA",
    "spark-daria": "#EF553B",
    "quinn": "#00CC96",
    "jodie": "#AB63FA",
    "levi": "#FFA15A",
    "falsa": "#19D3F3",
}


@timeit
def create_star_network(
    repos: list[str], stargazers: list[StargazerNetworkRecord]
) -> nx.Graph:
    start_time = time.time()
    print("creating star network...")
    random.seed(42)
    added_repos = []
    current_user = stargazers[0].user_name

    graph = nx.Graph()
    for repo in repos:
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
    elapsed_time = time.time() - start_time
    print(f"{elapsed_time:.2f} seconds")
    return graph


@timeit
def create_follower_network(follower_info: list[FollowerInfo], out_dir: Path) -> nx.Graph:
    random.seed(42)
    added_nodes = []
    graph = nx.Graph()
    for record in follower_info:
        if record.user_name not in added_nodes:
            graph.add_node(
                record.user_name,
                followers_count=record.followers_count,
                internal_followers_count=record.internal_followers_count,
            )
            added_nodes.append(record.user_name)

        if record.follower_name not in added_nodes:
            graph.add_node(
                record.follower_name,
                followers_count=record.follower_followers_count,
                internal_followers_count=record.follower_internal_followers_count,
            )
            added_nodes.append(record.follower_name)

        graph.add_edge(record.user_name, record.follower_name, weight=0.03)

    pos = nx.spring_layout(graph)
    nx.set_node_attributes(graph, pos, "pos")

    out_dir.mkdir(exist_ok=True, parents=True)
    out_path = out_dir / "follower_network.pkl"
    with out_path.open("wb") as f:
        pickle.dump(graph, f)

    return graph


@timeit
def create_star_network_plot(
    graph: nx.Graph, repos: list[str], stargazers: list[StargazerNetworkRecord]
) -> go.Figure:
    edge_x = []
    edge_y = []
    for edge in graph.edges():
        x0, y0 = graph.nodes[edge[0]]["pos"]
        x1, y1 = graph.nodes[edge[1]]["pos"]
        edge_x.append(x0)
        edge_x.append(x1)
        edge_x.append(None)
        edge_y.append(y0)
        edge_y.append(y1)
        edge_y.append(None)

    edge_trace = go.Scatter(
        x=edge_x,
        y=edge_y,
        line=dict(width=0.1, color="rgba(0, 0, 0, 0.2)"),
        hoverinfo="none",
        mode="lines",
        showlegend=False,
    )

    node_info = []
    for node in graph.nodes():
        node_data = graph.nodes.data()[node]
        repo = node_data["repo"]
        if node == repo:
            continue

        x, y = graph.nodes[node]["pos"]
        followers_count = node_data["followers_count"]
        user_name = node.split("_")[0]

        all_repos = [i.repo_name for i in stargazers if i.user_name == user_name]

        all_repos_text = ", ".join(all_repos)

        node_text_list = [
            user_name,
            f"followers={followers_count}",
            f"repos={all_repos_text}",
        ]

        node_text = "<br>".join(node_text_list)
        node_info.append(
            {
                "x": x,
                "y": y,
                "repo": repo,
                "text": node_text,
                "size": followers_count,
            }
        )

    node_df = pd.DataFrame(node_info)
    node_df["size_group"] = pd.qcut(node_df["size"], 6, labels=False, duplicates="drop")
    node_df["size_group"] = (node_df["size_group"] + 1) * 5

    all_node_traces = []
    for repo in repos:
        repo_df = node_df.query(f"repo == '{repo}'")
        node_trace = go.Scatter(
            x=repo_df.x,
            y=repo_df.y,
            marker_size=repo_df.size_group,
            mode="markers",
            hoverinfo="text",
            hovertext=repo_df.text,
            name=repo,
        )
        all_node_traces.append(node_trace)

    last_updated = max([i.retrieved_at for i in stargazers])
    last_updated_str = last_updated.strftime("%Y-%m-%d")

    title_text = f"mrpowers-io Stargazers<br><sup>last updated: {last_updated_str}</sup>"
    fig = go.Figure(
        data=[edge_trace, *all_node_traces],
        layout=go.Layout(
            title=title_text,
            showlegend=True,
            hovermode="closest",
            margin=dict(b=20, l=5, r=5, t=55),
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        ),
    )

    return fig


@timeit
def create_follower_network_plot(
    graph: nx.Graph, follower_info: list[FollowerInfo], last_updated: datetime.datetime
) -> go.Figure:
    all_connections = [(i.user_name, i.follower_name) for i in follower_info]
    solo_edges = {"x": [], "y": []}
    mutual_edges = {"x": [], "y": []}
    for edge in graph.edges():
        followed = edge[0]
        follower = edge[1]

        is_mutual = (follower, followed) in all_connections
        x0, y0 = graph.nodes[followed]["pos"]
        x1, y1 = graph.nodes[follower]["pos"]

        if is_mutual:
            mutual_edges["x"].extend([x0, x1, None])
            mutual_edges["y"].extend([y0, y1, None])
        else:
            solo_edges["x"].extend([x0, x1, None])
            solo_edges["y"].extend([y0, y1, None])

    solo_edge_trace = go.Scatter(
        x=solo_edges["x"],
        y=solo_edges["y"],
        line=dict(width=1, color="rgba(0, 0, 0, 0.3)"),
        hoverinfo="none",
        mode="lines",
        showlegend=False,
    )
    mutual_edge_trace = go.Scatter(
        x=mutual_edges["x"],
        y=mutual_edges["y"],
        line=dict(width=1, color="rgba(0, 117, 255, 0.8)"),
        hoverinfo="none",
        mode="lines",
        showlegend=False,
    )

    node_info = []
    for node in graph.nodes():
        node_data = graph.nodes.data()[node]
        x, y = graph.nodes[node]["pos"]
        followers_count = node_data["followers_count"]
        internal_followers_count = node_data["internal_followers_count"]
        if followers_count == 0:
            internal_followers_pct = 0
        else:
            internal_followers_pct = (
                node_data["internal_followers_count"] / followers_count
            )
        user_name = node

        node_text_list = [
            user_name,
            f"followers={followers_count}",
            f"internal_followers={internal_followers_count}",
            f"internal_followers_pct={internal_followers_pct:.2f}",
        ]

        node_text = "<br>".join(node_text_list)
        node_info.append(
            {
                "x": x,
                "y": y,
                "text": node_text,
                "followers_count": followers_count,
                "internal_followers_count": internal_followers_count,
                "internal_followers_pct": internal_followers_pct,
            }
        )

    node_df = pd.DataFrame(node_info)
    node_df["followers_group"] = pd.qcut(
        node_df["followers_count"],
        10,
        labels=False,
        duplicates="drop",
    )

    node_df["followers_group"] **= 8

    node_trace = go.Scatter(
        x=node_df.x,
        y=node_df.y,
        marker=dict(
            size=6,
            color=node_df.followers_group,
            colorscale="Greys",
            line=dict(width=1.1, color="black"),
        ),
        mode="markers",
        hoverinfo="text",
        hovertext=node_df.text,
    )

    last_updated_str = last_updated.strftime("%Y-%m-%d")

    title_text = (
        f"mrpowers-io Follower Network<br><sup>last updated: {last_updated_str}</sup>"
    )
    fig = go.Figure(
        data=[solo_edge_trace, mutual_edge_trace, node_trace],
        layout=go.Layout(
            title=title_text,
            showlegend=True,
            hovermode="closest",
            margin=dict(b=20, l=5, r=5, t=55),
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        ),
    )

    return fig


@timeit
def viz_star_network(use_cache: bool = True, show_fig: bool = False) -> Figure:
    con = get_db_con()
    stargazers = con.sql(
        """
        SELECT
        DISTINCT
        a.user_name,
        a.followers_count,
        b.starred_at,
        b.retrieved_at,
        c.repo_name
        FROM users a 
        INNER JOIN stargazers b
        ON a.user_id = b.user_id
        INNER JOIN repos c
        ON b.repo_id = c.repo_id
        ORDER BY b.user_name 
        """
    ).to_df()

    stargazers = list(StargazerNetworkRecord(*record) for record in stargazers.values)
    repos = sorted(set(i.repo_name for i in stargazers))
    out_dir = Path(__file__).parents[1] / "data" / "viz"
    out_path = out_dir / "star_network.pkl"

    if use_cache and out_path.exists():
        print("loading from cache")
        with out_path.open("rb") as f:
            network = pickle.load(f)
    else:
        print("creating from scratch")
        network = create_star_network(repos, stargazers)

    fig = create_star_network_plot(network, repos, stargazers)
    if show_fig:
        fig.show()

    return fig


@timeit
def viz_follower_network(use_cache: bool):
    con = get_db_con()
    follower_info = con.sql(
        """
        WITH internal_followers AS (
            SELECT
                user_id,
                count(DISTINCT follower_id)  internal_followers_count
            FROM followers
            GROUP BY user_id
        )
        SELECT DISTINCT
            b.user_name,
            b.followers_count,
            d.internal_followers_count,
            c.user_name  follower_name,
            c.followers_count  follower_followers_count,
            e.internal_followers_count  follower_internal_followers_count
        FROM followers a 

        INNER JOIN users b
        ON a.user_id = b.user_id
        INNER JOIN users c
        ON a.follower_id = c.user_id

        LEFT JOIN internal_followers d
        ON a.user_id = d.user_id
        LEFT JOIN internal_followers e
        ON a.follower_id = e.user_id

        ORDER BY b.user_name 
        """
    ).to_df()

    last_updated = (
        con.sql("SELECT max(retrieved_at)  retrieved_at FROM followers")
        .to_df()
        .to_dict()["retrieved_at"][0]
    )

    out_dir = Path(__file__).parents[1] / "data" / "viz"
    follower_info = list(FollowerInfo(*record) for record in follower_info.values)
    if use_cache:
        out_path = out_dir / "follower_network.pkl"
        with out_path.open("rb") as f:
            network = pickle.load(f)
    else:
        network = create_follower_network(follower_info, out_dir)

    fig = create_follower_network_plot(network, follower_info, last_updated)
    fig.show()


def viz_summary(show_fig: bool = False):
    con = get_db_con()
    df = con.sql("""SELECT
	repo_id,
	metric_type,
	metric_date,
	metric_count,
	repo_name
FROM main.mart_repo_summary
ORDER BY
	metric_date
    """).to_df()

    fig = px.line(
        df,
        x="metric_date",
        y="metric_count",
        color="repo_name",
        facet_col="metric_type",
        facet_col_wrap=2,
        template="simple_white",
        hover_name="repo_name",
        markers=True,
        color_discrete_map=REPO_PALETTE,
        height=350 * 6 / 2,
        category_orders={
            "metric_type": [
                "stars",
                "issues",
                "commits",
                "lines of code",
                "forks",
                "pull requests",
            ]
        },
    )

    fig.update_yaxes(matches=None, showticklabels=True)
    fig.update_traces(line=dict(width=1.75), marker=dict(size=4))
    fig.update_traces(hovertemplate="<b>%{x}</b><br>n=%{y}")
    fig.update_layout(legend=dict(title="<b>repo</b>"))
    fig.for_each_annotation(
        lambda a: a.update(text="<b>" + a.text.split("=")[-1] + "</b>")
    )
    fig.for_each_yaxis(lambda y: y.update(title=""))
    fig.for_each_xaxis(lambda x: x.update(title="", showticklabels=True))

    if show_fig:
        fig.show()

    return fig


if __name__ == "__main__":
    # viz_star_network()
    # viz_follower_network(use_cache=False)
    # viz_follower_network(use_cache=False)
    viz_summary()
