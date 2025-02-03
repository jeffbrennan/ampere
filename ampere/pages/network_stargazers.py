import os

import dash_bootstrap_components as dbc
import pandas as pd
from dash import Input, Output, callback, dash_table, dcc, html

from ampere.common import get_frontend_db_con, timeit
from ampere.styling import ScreenWidth, get_ampere_dt_style
from ampere.viz import read_plotly_fig_pickle, viz_star_network


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


@callback(
    [
        Output("network-stargazer-graph", "figure"),
        Output("network-stargazer-graph", "style"),
        Output("network-stargazer-table", "style"),
        Output("network-stargazer-graph-fade", "is_in"),
    ],
    [
        Input("color-mode-switch", "value"),
        Input("breakpoints", "widthBreakpoint"),
    ],
)
@timeit
def get_stylized_network_graph(dark_mode: bool, breakpoint_name: str):
    mode = "dark" if dark_mode else "light"
    env = os.environ.get("AMPERE_ENV")

    if env == "prod":
        fname = f"stargazer_network_{mode}_{breakpoint_name}"
        fig = read_plotly_fig_pickle(fname)
    else:
        fig = viz_star_network(
            dark_mode=dark_mode,
            screen_width=ScreenWidth(breakpoint_name),
        )

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
    [
        Input("color-mode-switch", "value"),
        Input("breakpoints", "widthBreakpoint"),
    ],
)
@timeit
def get_styled_stargazers_table(dark_mode: bool, breakpoint_name: str):
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
    return tbl


def layout():
    return [
        dbc.Fade(
            id="network-stargazer-graph-fade",
            children=[
                dcc.Graph(
                    id="network-stargazer-graph",
                    style={"visibility": "hidden"},
                    responsive=True,
                    config={"displayModeBar": False},
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
