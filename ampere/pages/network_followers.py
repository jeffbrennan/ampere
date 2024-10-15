import dash
import dash_bootstrap_components as dbc
from dash import dcc, callback, Output, Input
import dash_breakpoints
from plotly.graph_objects import Figure

from ampere.viz import viz_follower_network

dash.register_page(__name__)


def layout(**kwargs):
    return dbc.Row(
        [
            dcc.Interval(
                id="network-follower-load-interval",
                n_intervals=0,
                max_intervals=0,
                interval=1,
            ),
            dash_breakpoints.WindowBreakpoints(id="network-follower-breakpoints"),
            dcc.Loading(
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
            ),
        ]
    )


@callback(
    Output("network-follower-graph", "style"),
    Input("network-follower-breakpoints", "width"),
)
@callback(
    Output("network-follower-graph", "figure"),
    Input("network-follower-load-interval", "n_intervals"),
)
def show_summary_graph(_: int) -> Figure:
    return viz_follower_network()
