import dash
import dash_bootstrap_components as dbc
import pandas as pd
from dash import Input, Output, callback, dash_table, dcc, html
from plotly.graph_objects import Figure

from ampere.common import get_db_con
from ampere.styling import AmpereDTStyle
from ampere.viz import viz_star_network

dash.register_page(__name__, name="network", top_nav=True, order=1)


def create_stargazers_table() -> pd.DataFrame:
    con = get_db_con()
    return con.sql(
        """
        select
            user_name,
            name,
            followers,
            "starred repos",
            "spark-daria",
            quinn,
            "spark-fast-tests",
            jodie,
            levi,
            falsa
        from mart_stargazers_pivoted
        order by followers desc
        """
    ).to_df()


def layout():
    df = create_stargazers_table()
    return [
        html.Br(),
        dcc.Interval(
            id="network-stargazer-load-interval",
            n_intervals=0,
            max_intervals=0,
            interval=1,
        ),
        dbc.Fade(
            id="network-stargazer-graph-fade",
            children=dcc.Graph(
                id="network-stargazer-graph",
                style={
                    "height": "95vh",
                    "marginLeft": "0vw",
                    "marginRight": "0vw",
                    "width": "100%",
                },
                responsive=True,
            ),
            style={"transition": "opacity 1000ms ease"},
            is_in=False,
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
            **AmpereDTStyle,
        ),
    ]


@callback(
    [
        Output("network-stargazer-graph", "figure"),
        Output("network-stargazer-graph-fade", "is_in"),
    ],
    Input("network-stargazer-load-interval", "n_intervals"),
)
def show_summary_graph(_: int) -> tuple[Figure, bool]:
    return viz_star_network(), True
