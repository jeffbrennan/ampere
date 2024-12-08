import dash
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
            **AmpereDTStyle,
        ),
    ]


@callback(
    Output("network-stargazer-graph", "figure"),
    Input("network-stargazer-load-interval", "n_intervals"),
)
def show_summary_graph(_: int) -> Figure:
    return viz_star_network()
