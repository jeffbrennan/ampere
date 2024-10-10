import dash
import dash_bootstrap_components as dbc
from dash import dcc, callback, Output, Input
from plotly.graph_objects import Figure

from ampere.pages.side_bar import sidebar
from ampere.viz import viz_follower_network

dash.register_page(__name__)


def layout(**kwargs):
    return dbc.Row(
        [
            dbc.Col(sidebar(), width=1),
            dcc.Interval(
                id="network-follower-load-interval",
                n_intervals=0,
                max_intervals=0,
                interval=1,
            ),
            dbc.Col(
                dcc.Graph(
                    id="network-follower-graph",
                    style={"height": "95vh"},
                ),
                width=11,
            ),
        ]
    )


@callback(
    Output("network-follower-graph", "figure"),
    Input("network-follower-load-interval", "n_intervals"),
)
def show_summary_graph(_: int) -> Figure:
    return viz_follower_network()
