import dash
import dash_bootstrap_components as dbc
import pandas as pd
from dash import Input, Output, callback, dash_table, dcc, html
from plotly.graph_objects import Figure

from ampere.common import get_frontend_db_con
from ampere.styling import AmpereDTStyle
from ampere.viz import viz_follower_network

dash.register_page(__name__)


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


def layout():
    df = create_followers_table()
    return [
        html.Br(),
        dcc.Interval(
            id="network-follower-load-interval",
            n_intervals=0,
            max_intervals=0,
            interval=1,
        ),
        dbc.Fade(
            id="network-follower-graph-fade",
            children=[
                dcc.Graph(
                    id="network-follower-graph",
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
                    id="tbl",
                    **AmpereDTStyle,
                ),
            ],
            style={"transition": "opacity 500ms ease-in"},
            is_in=False,
        ),
    ]


@callback(
    [
        Output("network-follower-graph", "figure"),
        Output("network-follower-graph-fade", "is_in"),
    ],
    Input("network-follower-load-interval", "n_intervals"),
)
def show_summary_graph(_: int) -> tuple[Figure, bool]:
    return viz_follower_network(), True
