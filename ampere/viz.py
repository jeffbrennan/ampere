from __future__ import annotations

import datetime
import pickle
from pathlib import Path
from typing import Any, Optional

import networkx as nx
import pandas as pd
import plotly
import plotly.express as px
import plotly.graph_objects as go
import pypalettes
from plotly.graph_objs import Figure

from ampere.common import get_frontend_db_con, timeit
from ampere.get_repo_metrics import read_repos
from ampere.models import (
    FollowerDetails,
    Followers,
    StargazerNetworkRecord,
)
from ampere.styling import AmperePalette, ScreenWidth, get_ampere_colors


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


def read_pickle(pkl_name: str) -> Any:
    out_dir = Path(__file__).parents[1] / "data" / "viz"
    out_path = out_dir / f"{pkl_name}.pkl"
    with out_path.open("rb") as f:
        obj = pickle.load(f)
    return obj


def read_json(pkl_name: str) -> Any:
    out_dir = Path(__file__).parents[1] / "data" / "viz"
    out_path = out_dir / f"{pkl_name}.json"
    with out_path.open("r") as f:
        data = f.read()
    return data


def read_network_graph_pickle(pkl_name: str) -> nx.Graph:
    return read_pickle(pkl_name)


@timeit
def read_plotly_fig_pickle(pkl_name: str) -> Figure:
    return read_pickle(pkl_name)


@timeit
def read_dataframe_pickle(pkl_name: str) -> pd.DataFrame:
    return read_pickle(pkl_name)


@timeit
def read_plotly_fig_json(f_name: str) -> Figure:
    fig_data = read_json(f_name)
    return go.Figure(plotly.io.from_json(fig_data))


def filter_df_by_date_range(
    df: pd.DataFrame, col_name: str, date_range: Optional[list[int]] = None
) -> pd.DataFrame:
    if date_range is None:
        return df

    filter_date_min = datetime.datetime.fromtimestamp(date_range[0])
    filter_date_max = datetime.datetime.fromtimestamp(date_range[1])

    return df.query(f"{col_name} >= '{filter_date_min}'").query(
        f"{col_name} <= '{filter_date_max}'"
    )


def style_area_fig(fig: Figure, dark_mode: bool, screen_width: ScreenWidth) -> Figure:
    bg_color, font_color = get_ampere_colors(dark_mode, contrast=False)

    if screen_width == ScreenWidth.xs:
        legend_font_size = 12
        annotation_font_size = 14
        tick_font_size = 10

        fig.update_layout(
            legend=dict(
                title=None,
                itemsizing="constant",
                orientation="h",
                yanchor="top",
                y=1.475,
                xanchor="center",
                x=0.5,
                font=dict(size=legend_font_size, color=font_color),
            ),
            dragmode=False,
        )
    else:
        legend_font_size = 16
        annotation_font_size = 20
        tick_font_size = 16
        fig.update_layout(
            legend=dict(
                title=None,
                itemsizing="constant",
                font=dict(size=legend_font_size, color=font_color),
            )
        )
    fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
    fig.for_each_annotation(
        lambda a: a.update(
            text="<b>" + a.text.split("=")[-1].replace("_", " ") + "</b>",
            font_size=annotation_font_size,
            font_color=font_color,
            borderpad=5,
            y=1.002,
        )
    )

    fig.for_each_yaxis(
        lambda y: y.update(
            title="",
            showline=True,
            linewidth=2,
            linecolor=font_color,
            color=font_color,
            mirror=True,
            tickfont_size=tick_font_size,
        )
    )
    fig.for_each_xaxis(
        lambda x: x.update(
            title="",
            showline=True,
            linewidth=2,
            linecolor=font_color,
            color=font_color,
            mirror=True,
            showticklabels=True,
            tickfont_size=tick_font_size,
        )
    )
    fixed_axes = screen_width == ScreenWidth.xs
    fig.update_yaxes(
        matches=None, showticklabels=True, showgrid=False, fixedrange=fixed_axes
    )
    fig.update_xaxes(showgrid=False, fixedrange=fixed_axes)

    fig.update_layout(
        title={
            "y": 0.95,
            "x": 0.5,
            "xanchor": "center",
            "yanchor": "top",
        },
    )

    if screen_width != ScreenWidth.xs:
        fig.update_layout(
            margin=dict(l=0, r=200, t=50, b=0),
        )
    else:
        fig.update_layout(
            margin=dict(l=0, r=0, t=0, b=0),
        )

    return fig


def viz_summary(
    df: pd.DataFrame,
    metric_type: str,
    date_range: Optional[list[int]] = None,
    screen_width: ScreenWidth = ScreenWidth.lg,
    dark_mode: bool = False,
) -> Figure:
    df_filtered = df.query(f"metric_type == '{metric_type}'")

    df_filtered = filter_df_by_date_range(df_filtered, "metric_date", date_range)

    template = "plotly_dark" if dark_mode else "plotly_white"
    repo_palette = generate_repo_palette()
    fig = px.area(
        df_filtered,
        x="metric_date",
        y="metric_count",
        color="repo_name",
        template=template,
        hover_name="repo_name",
        color_discrete_map=repo_palette,
        height=500,
        category_orders={"repo_name": repo_palette.keys()},
        facet_col="metric_type",  # single var facet col for plot title
    )

    fig = style_area_fig(fig, dark_mode, screen_width)
    return fig


def get_summary_data() -> pd.DataFrame:
    with get_frontend_db_con() as con:
        df = con.sql(
            """
        select
            repo_name,
            metric_type,
            metric_date,
            metric_count,
        from main.mart_repo_summary
        order by metric_date
    """,
        ).to_df()

    return df


@timeit
def get_downloads_data(repo_name: str) -> pd.DataFrame:
    with get_frontend_db_con() as con:
        df = con.sql(
            f"""
            select
            repo,
            download_date,
            group_name,
            group_value,
            download_count
            from mart_downloads_summary
            where repo = '{repo_name}'
            order by download_date, download_count
            """,
        ).to_df()

    return df


@timeit
def viz_downloads(
    df: pd.DataFrame,
    group_name: str,
    date_range: Optional[list[int]] = None,
    dark_mode: bool = False,
    screen_width: ScreenWidth = ScreenWidth.lg,
) -> Figure:
    df_filtered = df.query(f"group_name=='{group_name}'")
    df_filtered = filter_df_by_date_range(df_filtered, "download_date", date_range)

    max_date = df_filtered["download_date"].max()
    categories = (
        df_filtered[(df_filtered["download_date"] == max_date)]
        .sort_values("download_count", ascending=False)["group_value"]
        .tolist()
    )

    template = "plotly_dark" if dark_mode else "plotly_white"
    fig = px.area(
        df_filtered,
        x="download_date",
        y="download_count",
        color="group_value",
        facet_col="group_name",
        color_discrete_sequence=px.colors.qualitative.T10,
        template=template,
        category_orders={"group_value": categories},
    )

    fig = style_area_fig(fig, dark_mode, screen_width)

    if group_name == "overall":
        fig.update_layout(showlegend=False)

    return fig


@timeit
def create_star_network_plot(
    graph: nx.Graph,
    repos: list[str],
    stargazers: list[StargazerNetworkRecord],
    dark_mode: bool,
    screen_width: ScreenWidth,
) -> go.Figure:
    if dark_mode:
        edge_color = "rgba(242, 240, 227, 0.3)"
        legend_text_color = AmperePalette.BRAND_TEXT_COLOR_DARK
    else:
        edge_color = "rgba(33, 33, 33, 0.3)"
        legend_text_color = AmperePalette.BRAND_TEXT_COLOR_LIGHT

    edge_x = []
    edge_y = []
    for edge in graph.edges():
        if edge[0] in repos:
            continue

        x0, y0 = graph.nodes[edge[0]]["pos"]
        x1, y1 = graph.nodes[edge[1]]["pos"]

        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])

    edge_trace = go.Scatter(
        x=edge_x,
        y=edge_y,
        line=dict(width=1, color=edge_color),
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
            f"<b> {user_name} </b>",
            "",
            f"followers: {followers_count}",
            f"repos: {all_repos_text}",
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

    repo_palette = generate_repo_palette()
    all_node_traces = []
    for repo in repos:
        repo_df = node_df.query(f"repo == '{repo}'")
        node_trace = go.Scatter(
            x=repo_df.x,
            y=repo_df.y,
            marker_size=repo_df.size_group,
            marker_color=repo_palette[repo],
            mode="markers",
            hoverinfo="text",
            hovertext=repo_df.text,
            name=repo,
        )
        all_node_traces.append(node_trace)

    fig = go.Figure(data=[edge_trace, *all_node_traces], layout=NETWORK_LAYOUT)

    legend_y = 1.3 if screen_width == ScreenWidth.xs else 1.02
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        legend=dict(
            font=dict(color=legend_text_color, size=16),
            y=legend_y,
        ),
    )

    if screen_width == ScreenWidth.xs:
        fig.update_xaxes(fixedrange=True)
        fig.update_yaxes(fixedrange=True)
        fig.update_layout(dragmode=False)
    return fig


@timeit
def viz_star_network(dark_mode: bool, screen_width: ScreenWidth) -> Figure:
    with get_frontend_db_con() as con:
        stargazers = con.sql(
            """
        select
            user_name,
            followers_count,
            starred_at,
            retrieved_at,
            repo_name
        from int_network_stargazers,
    """,
        ).to_df()

    stargazers = list(StargazerNetworkRecord(*record) for record in stargazers.values)
    repos_with_stargazers = list(set(i.repo_name for i in stargazers))
    repo_palette = generate_repo_palette()
    repos = [i for i in repo_palette if i in repos_with_stargazers]

    network = read_network_graph_pickle("star_network")
    fig = create_star_network_plot(network, repos, stargazers, dark_mode, screen_width)
    return fig


@timeit
def create_follower_network_plot(
    graph: nx.Graph,
    follower_info: list[Followers],
    follower_details: dict[int, FollowerDetails],
    dark_mode: bool,
    screen_width: ScreenWidth,
) -> go.Figure:
    if dark_mode:
        edge_color = "rgba(242, 240, 227, 0.5)"
        legend_text_color = AmperePalette.BRAND_TEXT_COLOR_DARK
    else:
        edge_color = "rgba(33, 33, 33, 0.5)"
        legend_text_color = AmperePalette.BRAND_TEXT_COLOR_LIGHT

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
        line=dict(width=1, color="rgb(247, 111, 83)"),
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
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        legend=dict(font=dict(color=legend_text_color, size=16)),
    )

    if screen_width == ScreenWidth.xs:
        fig.update_xaxes(fixedrange=True)
        fig.update_yaxes(fixedrange=True)
        fig.update_layout(dragmode=False)
    return fig


@timeit
def viz_follower_network(dark_mode: bool, screen_width: ScreenWidth) -> Figure:
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
        network, followers, follower_details, dark_mode, screen_width
    )
    return fig


NETWORK_LAYOUT = go.Layout(
    showlegend=True,
    hovermode="closest",
    xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
    yaxis=dict(
        showgrid=False,
        zeroline=False,
        showticklabels=False,
    ),
    template="none",
    margin=dict(t=0, l=0, r=0, b=0),
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
