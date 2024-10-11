import dash
import dash_bootstrap_components as dbc
from dash import dcc, callback, Output, Input
import dash_breakpoints
from plotly.graph_objects import Figure

from ampere.pages.side_bar import sidebar
from ampere.viz import viz_follower_network

dash.register_page(__name__)


def layout(**kwargs):
    return dbc.Row(
        [
            dbc.Col(sidebar(), width=2),
            dcc.Interval(
                id="network-follower-load-interval",
                n_intervals=0,
                max_intervals=0,
                interval=1,
            ),
            dash_breakpoints.WindowBreakpoints(id="network-follower-breakpoints"),
            dbc.Col(dcc.Loading(dcc.Graph(id="network-follower-graph")), width=10),
        ]
    )


@callback(
    Output("network-follower-graph", "style"),
    Input("network-follower-breakpoints", "width"),
)
def handle_follower_widescreen(display_width_px: int):
    is_widescreen = display_width_px > 1920

    if is_widescreen:
        return {
            "height": "95vh",
            "marginLeft": "0vw",
            "marginRight": "20vw",
        }

    return {
        "height": "95vh",
        "marginLeft": "0vw",
        "marginRight": "0vw",
    }


@callback(
    Output("network-follower-graph", "figure"),
    Input("network-follower-load-interval", "n_intervals"),
)
def show_summary_graph(_: int) -> Figure:
    return viz_follower_network()
