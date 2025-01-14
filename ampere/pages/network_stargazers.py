import dash_bootstrap_components as dbc
import networkx as nx
import pandas as pd
import plotly.graph_objects as go
from dash import Input, Output, callback, dash_table, dcc, html
from plotly.graph_objects import Figure
from plotly.graph_objs import Figure

from ampere.common import get_frontend_db_con, timeit
from ampere.models import StargazerNetworkRecord
from ampere.styling import AmperePalette, get_ampere_dt_style
from ampere.viz import NETWORK_LAYOUT, generate_repo_palette, read_network_graph_pickle


def create_stargazers_table() -> pd.DataFrame:
    with get_frontend_db_con() as con:
        # select * because columns are dynamically generated
        df = con.sql(
            """
        select *
        from mart_stargazers_pivoted
        order by followers desc
        """,
        ).to_df()
    return df


@timeit
def create_star_network_plot(
    graph: nx.Graph,
    repos: list[str],
    stargazers: list[StargazerNetworkRecord],
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

    fig.update_layout(
        plot_bgcolor=background_color,
        paper_bgcolor=background_color,
        legend=dict(font=dict(color=legend_text_color)),
    )
    return fig


@timeit
def viz_star_network(dark_mode: bool) -> Figure:
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
    fig = create_star_network_plot(network, repos, stargazers, dark_mode)
    return fig


@callback(
    [
        Output("network-stargazer-graph", "figure"),
        Output("network-stargazer-graph", "style"),
        Output("network-stargazer-table", "style"),
        Output("network-stargazer-graph-fade", "is_in"),
    ],
    Input("color-mode-switch", "value"),
)
@timeit
def get_stylized_network_graph(dark_mode: bool):
    fig = viz_star_network(dark_mode)
    return (
        fig,
        {
            "height": "95vh",
            "marginLeft": "0vw",
            "marginRight": "0vw",
            "width": "100%",
        },
        {},
        True,
    )


@callback(
    Output("network-stargazer-table", "children"),
    Input("color-mode-switch", "value"),
)
@timeit
def get_styled_stargazers_table(dark_mode: bool):
    base_style = get_ampere_dt_style(dark_mode)
    df = create_stargazers_table()
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
    tbl = (
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
            **base_style,
        ),
    )
    return tbl


def layout():
    return [
        dbc.Fade(
            id="network-stargazer-graph-fade",
            children=[
                html.Br(),
                dcc.Graph(
                    id="network-stargazer-graph",
                    style={"visibility": "hidden"},
                    responsive=True,
                ),
                html.Div(id="network-stargazer-table", style={"visibility": "hidden"}),
            ],
            style={
                "transition": "opacity 200ms ease-in",
                "minHeight": "100vh",
            },
            is_in=False,
        ),
    ]
