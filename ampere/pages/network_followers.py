from dataclasses import dataclass
from typing import Optional

import dash
import dash_bootstrap_components as dbc
import networkx as nx
import pandas as pd
import plotly.graph_objects as go
from dash import Input, Output, callback, dash_table, dcc, html
from plotly.graph_objects import Figure
from plotly.graph_objs import Figure

from ampere.common import get_frontend_db_con, timeit
from ampere.models import Followers
from ampere.styling import AmperePalette, get_ampere_dt_style
from ampere.viz import NETWORK_LAYOUT, format_plot_name_list, read_network_graph_pickle

dash.register_page(__name__)


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
def create_follower_network_plot(
    graph: nx.Graph,
    follower_info: list[Followers],
    follower_details: dict[int, FollowerDetails],
    dark_mode: bool,
) -> go.Figure:
    if dark_mode:
        edge_color = "rgba(255, 255, 255, 0.3)"
        background_color = AmperePalette.PAGE_BACKGROUND_COLOR_DARK
        legend_text_color = "white"
    else:
        edge_color = "rgba(0, 0, 0, 0.3)"
        background_color = AmperePalette.PAGE_BACKGROUND_COLOR_LIGHT
        legend_text_color = "black"

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
        line=dict(width=1, color=edge_color),
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
    if dark_mode:
        node_df["followers_group"] = 10 - node_df["followers_group"]
    
    node_trace = go.Scatter(
        x=node_df.x,
        y=node_df.y,
        marker=dict(
            size=8,
            color=node_df.followers_group,
            colorscale="Greys",
            line=dict(width=1, color=legend_text_color),
        ),
        mode="markers",
        hoverinfo="text",
        hovertext=node_df.text,
        name="follower count",
    )

    fig = go.Figure(
        data=[solo_edge_trace, mutual_edge_trace, node_trace], layout=NETWORK_LAYOUT
    )

    fig.update_layout(
        plot_bgcolor=background_color,
        paper_bgcolor=background_color,
        legend=dict(font=dict(color=legend_text_color)),
    )
    return fig


@timeit
def viz_follower_network(dark_mode: bool) -> Figure:
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

    fig = create_follower_network_plot(
        network,
        followers,
        follower_details,
        dark_mode,
    )
    return fig


def create_followers_table() -> pd.DataFrame:
    with get_frontend_db_con() as con:
        df = con.sql(
            """
        select
            concat('[', user_name, ']', '(https://www.github.com/', user_name, ')') as user_name,
            full_name                                                               as name,
            followers_count                                                         as followers,
            internal_followers_count                                                as "org followers",
            round(internal_followers_pct * 100.0, 2)                                as "org followers %",
            following_count                                                         as following,
            internal_following_count                                                as "org following",
            round(internal_following_pct * 100.0, 2)                                as "org following %"
        from int_network_follower_details
        order by followers_count desc
        """
        ).to_df()
    return df


@callback(
    [
        Output("followers-table", "style_table"),
        Output("followers-table", "style_cell_conditional"),
        Output("followers-table", "style_data_conditional"),
        Output("followers-table", "style_filter"),
        Output("followers-table", "style_header"),
        Output("followers-table", "css"),
    ],
    [
        Input("followers-table", "data"),
        Input("color-mode-switch", "value"),
    ],
)
def style_followers_table(data: list[dict], dark_mode: bool):
    base_style = get_ampere_dt_style(dark_mode)
    df = pd.DataFrame(data)
    if dark_mode:
        text_color = "white"
    else:
        text_color = "black"

    standard_col_colors = [
        {
            "color": text_color,
            "borderLeft": "none",
            "borderRight": f"2px solid {text_color}",
        }
        for _ in df.columns
    ]
    base_style["style_data_conditional"] += standard_col_colors
    return (
        base_style["style_table"],
        base_style["style_cell_conditional"],
        base_style["style_data_conditional"],
        base_style["style_filter"],
        base_style["style_header"],
        base_style["css"],
    )


def layout():
    df = create_followers_table()
    return [
        html.Br(),
        dcc.Interval(
            id="network-followers-load-interval",
            n_intervals=0,
            max_intervals=0,
            interval=1,
        ),
        dbc.Fade(
            id="network-followers-graph-fade",
            children=[
                dcc.Graph(
                    id="network-followers-graph",
                    style={
                        "height": "95vh",
                        "marginLeft": "0vw",
                        "marginRight": "0vw",
                        "width": "100%",
                    },
                    responsive=True,
                ),
                dash_table.DataTable(
                    df.to_dict("records"),
                    columns=[
                        (
                            {"id": x, "name": "", "presentation": "markdown"}
                            if x == "user_name"
                            else {"id": x, "name": x}
                        )
                        for x in df.columns
                    ],
                    id="followers-table",
                    **get_ampere_dt_style(),
                ),
            ],
            style={"transition": "opacity 200ms ease-in"},
            is_in=False,
        ),
    ]


@callback(
    [
        Output("network-followers-graph", "figure"),
        Output("network-followers-graph-fade", "is_in"),
    ],
    Input("color-mode-switch", "value"),
)
def show_summary_graph(dark_mode: bool) -> tuple[Figure, bool]:
    return viz_follower_network(dark_mode), True
