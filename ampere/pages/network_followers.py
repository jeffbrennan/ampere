import os

import dash_bootstrap_components as dbc
import pandas as pd
from dash import Input, Output, callback, dash_table, dcc, html

from ampere.common import get_frontend_db_con, timeit
from ampere.styling import ScreenWidth, get_ampere_dt_style
from ampere.viz import (
    read_plotly_fig_pickle,
    viz_follower_network,
)


@timeit
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
        Output("followers-table", "children"),
        Output("followers-table", "style"),
    ],
    [
        Input("color-mode-switch", "value"),
        Input("breakpoints", "widthBreakpoint"),
    ],
)
def get_styled_followers_table(dark_mode: bool, breakpoint_name: str):
    base_style = get_ampere_dt_style(dark_mode)
    df = create_followers_table()
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
    if breakpoint_name in [ScreenWidth.xs, ScreenWidth.sm]:
        base_style["style_cell"]["font_size"] = "12px"

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
    return tbl, {}


@callback(
    [
        Output("network-followers-graph", "figure"),
        Output("network-followers-graph", "style"),
        Output("network-followers-graph-fade", "is_in"),
    ],
    [
        Input("color-mode-switch", "value"),
        Input("breakpoints", "widthBreakpoint"),
    ],
)
@timeit
def show_summary_graph(dark_mode: bool, breakpoint_name: str):
    mode = "dark" if dark_mode else "light"
    env = os.environ.get("AMPERE_ENV")

    if env == "prod":
        fig = read_plotly_fig_pickle(f"follower_network_{mode}_{breakpoint_name}")
    else:
        fig = viz_follower_network(dark_mode, ScreenWidth(breakpoint_name))

    return (
        fig,
        {
            "height": "95vh",
            "marginLeft": "0vw",
            "marginRight": "0vw",
            "width": "100%",
        },
        True,
    )


def layout():
    return [
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
                        "visibility": "hidden",
                    },
                    responsive=True,
                    config={"displayModeBar": False},
                ),
                html.Br(),
                html.Div(id="followers-table", style={"visibility": "hidden"}),
            ],
            style={"transition": "opacity 200ms ease-in", "minHeight": "100vh"},
            is_in=False,
        ),
    ]
