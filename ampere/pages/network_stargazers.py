import dash
import dash_breakpoints
import pandas as pd
from dash import Input, Output, callback, dash_table, dcc
from plotly.graph_objects import Figure

from ampere.common import Palette, get_db_con
from ampere.viz import viz_star_network

dash.register_page(__name__, name="network", top_nav=True, order=1)


def create_stargazers_table() -> pd.DataFrame:
    con = get_db_con()
    df = con.sql(
        """
        SELECT *
        FROM mart_stargazers_pivoted
        order by followers desc
        """
    ).to_df()
    return df


def layout(**kwargs):
    df = create_stargazers_table()
    return [
        dcc.Interval(
            id="network-stargazer-load-interval",
            n_intervals=0,
            max_intervals=0,
            interval=1,
        ),
        dash_breakpoints.WindowBreakpoints(id="network-stargazer-breakpoints"),
        dcc.Loading(
            dcc.Graph(
                id="network-stargazer-graph",
                style={
                    "height": "95vh",
                    "marginLeft": "0vw",
                    "marginRight": "0vw",
                    "width": "100%",
                },
                responsive=True,
            )
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
            id="tbl",
            sort_action="native",
            sort_mode="multi",
            column_selectable="single",
            row_selectable=False,
            row_deletable=False,
            fixed_rows={"headers": True},
            filter_action="native",
            filter_options={"case": "insensitive"},
            style_filter={"border": "0"},
            page_size=100,
            style_header={
                "backgroundColor": Palette.PAGE_ACCENT_COLOR,
                "padding": "10px",
                "color": "#FFFFFF",
                "fontWeight": "bold",
                "border": f"1px solid {Palette.PAGE_ACCENT_COLOR}",
            },
            style_cell={
                "textAlign": "center",
                "minWidth": 95,
                "maxWidth": 95,
                "width": 95,
                "font_size": "1em",
                "whiteSpace": "normal",
                "height": "auto",
                "font-family": "sans-serif",
                "borderTop": "0",
                "borderBottom": "0",
                "borderLeft": "2px solid black",
                "borderRight": "2px solid black",
            },
            style_data={"color": "black", "backgroundColor": "white"},
            style_data_conditional=[
                {
                    "if": {"row_index": "odd"},
                    "backgroundColor": "rgb(220, 220, 220)",
                }
            ],
            css=[dict(selector="p", rule="margin-bottom: 0; text-align: right;")],
            style_table={
                "height": "50%",
                "overflowY": "scroll",
                "overflowX": "scroll",
                "margin": {"b": 100},
            },
        ),
    ]


@callback(
    Output("network-stargazer-graph", "figure"),
    Input("network-stargazer-load-interval", "n_intervals"),
)
def show_summary_graph(_: int) -> Figure:
    return viz_star_network()
