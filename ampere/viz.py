import datetime
import pickle
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import networkx as nx
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import pypalettes
import pytz
from plotly.graph_objs import Figure

from ampere.common import get_frontend_db_con, timeit
from ampere.get_repo_metrics import read_repos
from ampere.models import Followers, StargazerNetworkRecord
from ampere.styling import AmperePalette, ScreenWidth


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


@timeit
def generate_repo_palette() -> dict[str, str]:
    with get_frontend_db_con() as con:
        repos = sorted(
            read_repos(con),
            key=lambda x: x.stargazers_count,
            reverse=True,
        )

    n_colors = 10
    n_repos = len(repos)
    repeats = (n_repos // n_colors) + 1

    colors = list(
        pypalettes.load_cmap("Tableau_10", cmap_type="discrete", repeat=repeats).rgb  # type: ignore
    )[0:n_repos]

    output = {}
    for i, repo in enumerate(repos):
        rgb_string = ", ".join(str(x) for x in colors[i])
        output[repo.repo_name] = f"rgb({rgb_string})"

    return output



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
            "",
            f"org followers: {node_details.internal_followers_count}/{node_details.followers_count} ({node_details.internal_followers_pct * 100:.02f}%)",
            f"org following: {node_details.internal_following_count}/{node_details.following_count} ({node_details.internal_following_pct * 100:.02f}%)",
        ]

        internal_followers_clean = format_plot_name_list(node_details.followers)
        internal_following_clean = format_plot_name_list(node_details.following)

        if internal_followers_clean is not None or internal_following_clean is not None:
            node_text_list += [""]

        if internal_followers_clean is not None:
            node_text_list += [f"followers: {internal_followers_clean}"]

        if internal_following_clean is not None:
            node_text_list += [f"following: {internal_following_clean}"]

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
        node_df["followers_count"],
        10,
        labels=False,
        duplicates="drop",
    )

    node_df["followers_group"] **= 10

    node_trace = go.Scatter(
        x=node_df.x,
        y=node_df.y,
        marker=dict(
            size=8,
            color=node_df.followers_group,
            colorscale="Greys",
            line=dict(width=1, color="black"),
        ),
        mode="markers",
        hoverinfo="text",
        hovertext=node_df.text,
        name="follower count",
    )

    fig = go.Figure(
        data=[solo_edge_trace, mutual_edge_trace, node_trace], layout=NETWORK_LAYOUT
    )

    return fig


def read_network_graph_pickle(pkl_name: str) -> nx.Graph:
    out_dir = Path(__file__).parents[1] / "data" / "viz"
    out_path = out_dir / f"{pkl_name}.pkl"
    with out_path.open("rb") as f:
        network = pickle.load(f)
    return network



@timeit
def viz_follower_network(show_fig: bool = False) -> Figure:
    with get_frontend_db_con() as con:
        followers = (
            con.sql("select user_id, follower_id from int_internal_followers")
            .to_df()
            .to_dict(orient="records")
        )

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
        """,
            )
            .to_df()
            .to_dict(orient="records")
        )

    followers = [Followers(**record) for record in followers]  # type: ignore
    follower_details_dc = [FollowerDetails(**i) for i in follower_details_dict]  # type: ignore
    follower_details = {i.user_id: i for i in follower_details_dc}

    # reads precomputed network graph from scheduled job
    network = read_network_graph_pickle("follower_network")

    print("creating plot..")
    fig = create_follower_network_plot(network, followers, follower_details)
    if show_fig:
        fig.show()

    return fig




NETWORK_LAYOUT = go.Layout(
    showlegend=True,
    hovermode="closest",
    margin=dict(b=20, l=0, r=0, t=55),
    xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
    yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
    template="none",
    legend=dict(
        title=None,
        itemsizing="constant",
        font=dict(size=14),
        orientation="h",
        yanchor="top",
        y=1.04,
        xanchor="center",
        x=0.5,
    ),
)
