import dash
import dash_breakpoints
import pandas as pd

from dash import Input, Output, callback, dcc, dash_table
from plotly.graph_objects import Figure

from ampere.common import get_db_con
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
            page_size=100,
            style_header={
                "backgroundColor": "#3F6DF9",
                "padding": "10px",
                "color": "#FFFFFF",
                "fontWeight": "bold",
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
            },
            style_data_conditional=[
                {
                    "if": {
                        "filter_query": f"{{{i}}} is blank",
                        "column_id": i,
                    },
                    "backgroundColor": "#D3D3D3",
                }
                for i in df.columns
                if i not in ["user_name", "name", "followers"]
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
