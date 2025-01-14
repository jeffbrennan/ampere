import dash_bootstrap_components as dbc
import pandas as pd
from dash import Input, Output, callback, dash_table, dcc, html

from ampere.common import get_frontend_db_con, timeit
from ampere.styling import get_ampere_dt_style
from ampere.viz import NETWORK_LAYOUT, read_plotly_fig_pickle


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
    Input("color-mode-switch", "value"),
)
@timeit
def get_stylized_network_graph(dark_mode: bool):
    mode = "dark" if dark_mode else "light"
    fname = f"stargazer_network_{mode}"
    fig = read_plotly_fig_pickle(fname)

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
