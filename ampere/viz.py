import datetime
import pickle
import random
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import networkx as nx
import numpy
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
class Followers:
    user_id: str
    follower_id: int


@dataclass(slots=True, frozen=True)
class FollowerDetails:
    user_id: int
    user_name: str
    followers_count: int
    following_count: int
    followers: Optional[list[str]]
    following: Optional[list[str]]
    internal_followers_count: int
    internal_following_count: int
    internal_followers_pct: float
    internal_following_pct: float


# intended for traces to appear in this order
REPO_PALETTE = {
    "spark-daria": "#EF553B",
    "quinn": "#00CC96",
    "spark-fast-tests": "#636EFA",
    "jodie": "#AB63FA",
    "levi": "#FFA15A",
    "falsa": "#19D3F3",
    "community": "#D57DBF",
}


@timeit
def create_star_network(
    repos: list[str], stargazers: list[StargazerNetworkRecord], out_dir: Path
) -> nx.Graph:
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

    out_dir.mkdir(exist_ok=True, parents=True)
    out_path = out_dir / "star_network.pkl"
    with out_path.open("wb") as f:
        pickle.dump(graph, f)

    return graph


@timeit
def create_follower_network(follower_info: list[Followers], out_dir: Path) -> nx.Graph:
    random.seed(42)
    added_nodes = []
    graph = nx.Graph()
    for record in follower_info:
        if record.user_id not in added_nodes:
            graph.add_node(record.user_id)
            added_nodes.append(record.user_id)

        if record.follower_id not in added_nodes:
            graph.add_node(record.follower_id)
            added_nodes.append(record.follower_id)

        graph.add_edge(record.user_id, record.follower_id, weight=0.03)
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
        line=dict(width=1, color="rgba(0, 0, 0, 0.3)"),
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
            marker_color=REPO_PALETTE[repo],
            mode="markers",
            hoverinfo="text",
            hovertext=repo_df.text,
            name=repo,
        )
        all_node_traces.append(node_trace)

    fig = go.Figure(
        data=[edge_trace, *all_node_traces],
        layout=go.Layout(
            showlegend=True,
            hovermode="closest",
            margin=dict(b=20, l=5, r=5, t=55),
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            template="none",
            legend=dict(font=dict(size=14)),
        ),
    )

    return fig


def format_plot_name_list(
    names: list[str] | float | None, max_names: int = 5
) -> Optional[str]:
    if names is None or isinstance(names, float) or isinstance(names, int):
        return None

    names_clean = names[0 : min(max_names, len(names))]

    names_clean_str = ", ".join(names_clean)
    if len(names) > max_names:
        names_clean_str += "..."

    return names_clean_str


@timeit
def create_follower_network_plot(
    graph: nx.Graph,
    follower_info: list[Followers],
    follower_details: dict[int, FollowerDetails],
) -> go.Figure:
    all_connections = [(i.user_id, i.follower_id) for i in follower_info]
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
        name="solo connection",
    )
    mutual_edge_trace = go.Scatter(
        x=mutual_edges["x"],
        y=mutual_edges["y"],
        line=dict(width=1, color="rgba(0, 117, 255, 0.8)"),
        hoverinfo="none",
        mode="lines",
        name="mutual connection",
    )

    node_info = []
    for node in graph.nodes():
        x, y = graph.nodes[node]["pos"]
        node_details = follower_details[node]
        node_text_list = [
            "<b>" + node_details.user_name + "</b>",
            f"followers_count={node_details.followers_count}",
            f"internal_followers_count={node_details.internal_followers_count}",
            f"internal_followers_pct={node_details.internal_followers_pct}",
            "",
            f"following_count={node_details.following_count}",
            f"internal_following_count={node_details.internal_following_count}",
            f"internal_following_pct={node_details.internal_following_pct}",
        ]

        internal_followers_clean = format_plot_name_list(node_details.followers)
        internal_following_clean = format_plot_name_list(node_details.following)

        if internal_followers_clean is not None or internal_following_clean is not None:
            node_text_list += [""]

        if internal_followers_clean is not None:
            node_text_list += [f"followers={internal_followers_clean}"]

        if internal_following_clean is not None:
            node_text_list += [f"following={internal_following_clean}"]

        node_text = "<br>".join(node_text_list)
        node_info.append(
            {
                "x": x,
                "y": y,
                "text": node_text,
                "followers_count": node_details.followers_count,
                "internal_followers_count": node_details.internal_followers_count,
                "internal_followers_pct": node_details.internal_followers_pct,
                "following_count": node_details.following_count,
                "internal_following_count": node_details.internal_following_count,
                "internal_following_pct": node_details.internal_following_pct,
            }
        )

    node_df = pd.DataFrame(node_info)
    node_df["followers_group"] = pd.qcut(
        node_df["internal_followers_count"],
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
        name="follower count",
    )

    fig = go.Figure(
        data=[solo_edge_trace, mutual_edge_trace, node_trace],
        layout=go.Layout(
            showlegend=True,
            hovermode="closest",
            margin=dict(b=20, l=5, r=5, t=55),
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            template="none",
            legend=dict(font=dict(size=14)),
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
        ORDER BY a.user_name 
        """
    ).to_df()

    stargazers = list(StargazerNetworkRecord(*record) for record in stargazers.values)
    repos_with_stargazers = list(set(i.repo_name for i in stargazers))
    repos = [i for i in REPO_PALETTE if i in repos_with_stargazers]
    out_dir = Path(__file__).parents[1] / "data" / "viz"
    out_path = out_dir / "star_network.pkl"

    if use_cache and out_path.exists():
        print("loading from cache")
        with out_path.open("rb") as f:
            network = pickle.load(f)
    else:
        print("creating from scratch")
        network = create_star_network(repos, stargazers, out_dir)

    fig = create_star_network_plot(network, repos, stargazers)
    if show_fig:
        fig.show()

    return fig


@timeit
def viz_follower_network(use_cache: bool = True, show_fig: bool = False) -> Figure:
    con = get_db_con()
    followers = (
        con.sql("SELECT user_id, follower_id FROM int_internal_followers")
        .to_df()
        .to_dict(orient="records")
    )
    followers = list(Followers(**record) for record in followers)  # type: ignore

    follower_details_dict = (
        con.sql(
            """
        select
        user_id,
        user_name,
        followers_count,
        following_count,
        followers,
        following,
        internal_followers_count,
        internal_following_count,
        internal_followers_pct,
        internal_following_pct
        from int_network_follower_details
        """
        )
        .to_df()
        .to_dict(orient="records")
    )
    follower_details_dc = [FollowerDetails(**i) for i in follower_details_dict]  # type: ignore
    follower_details = {i.user_id: i for i in follower_details_dc}

    out_dir = Path(__file__).parents[1] / "data" / "viz"
    out_path = out_dir / "follower_network.pkl"
    if use_cache and out_path.exists():
        with out_path.open("rb") as f:
            network = pickle.load(f)
    else:
        print("creating follower network...")
        network = create_follower_network(followers, out_dir)

    print("creating plot..")
    fig = create_follower_network_plot(network, followers, follower_details)
    if show_fig:
        fig.show()

    return fig


def viz_summary(show_fig: bool = False, screen_width_px: int = 1920):
    con = get_db_con()
    df = con.sql("""
    SELECT
        repo_id,
        metric_type,
        metric_date,
        metric_count,
        repo_name
    FROM main.mart_repo_summary
    ORDER BY metric_date
    """).to_df()

    metric_type_order = [
        "stars",
        "issues",
        "commits",
        "lines of code",
        "forks",
        "pull requests",
    ]

    if screen_width_px < 1200:
        facet_col_wrap = 1
        facet_row_spacing = 0.04
    elif screen_width_px < 2560:
        facet_col_wrap = 2
        facet_row_spacing = 0.10
    else:
        facet_col_wrap = 3
        facet_row_spacing = 0.10
        metric_type_order = [
            "stars",
            "forks",
            "commits",
            "lines of code",
            "issues",
            "pull requests",
        ]

    fig = px.line(
        df,
        x="metric_date",
        y="metric_count",
        color="repo_name",
        facet_col="metric_type",
        facet_col_wrap=facet_col_wrap,
        template="simple_white",
        hover_name="repo_name",
        markers=True,
        color_discrete_map=REPO_PALETTE,
        height=550 * 6 // facet_col_wrap,
        facet_col_spacing=0.08,
        facet_row_spacing=facet_row_spacing,
        category_orders={
            "metric_type": metric_type_order,
            "repo_name": REPO_PALETTE.keys(),
        },
    )

    fig.update_yaxes(matches=None, showticklabels=True)
    fig.update_traces(
        line=dict(width=1), marker=dict(size=5), hovertemplate="<b>%{x}</b><br>n=%{y}"
    )

    if screen_width_px < 1200:
        fig.update_layout(
            legend=dict(
                title=None,
                itemsizing="constant",
                font=dict(size=14),
                orientation="h",
                yanchor="top",
                y=1.05,
            )
        )
    else:
        fig.update_layout(
            legend=dict(title=None, itemsizing="constant", font=dict(size=14))
        )

    fig.for_each_annotation(
        lambda a: a.update(text="<b>" + a.text.split("=")[-1] + "</b>", font_size=14)
    )

    fig.for_each_yaxis(
        lambda y: y.update(
            title="",
            showline=True,
            linewidth=1,
            linecolor="black",
            mirror=True,
            tickfont_size=14,
        )
    )
    fig.for_each_xaxis(
        lambda x: x.update(
            title="",
            showline=True,
            linewidth=1,
            linecolor="black",
            mirror=True,
            showticklabels=True,
            tickfont_size=14,
        )
    )
    if show_fig:
        fig.show()

    return fig


if __name__ == "__main__":
    # viz_star_network()
    # viz_follower_network(use_cache=False)
    # viz_follower_network(use_cache=False)
    viz_summary()
