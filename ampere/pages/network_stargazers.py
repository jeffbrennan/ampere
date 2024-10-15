import dash
import dash_bootstrap_components as dbc
from dash import dcc, callback, Output, Input
import dash_breakpoints
from plotly.graph_objects import Figure

from ampere.viz import viz_star_network

dash.register_page(__name__, name="network", top_nav=True, order=1)


def layout(**kwargs):
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
    ]



@callback(
    Output("network-stargazer-graph", "figure"),
    Input("network-stargazer-load-interval", "n_intervals"),
)
def show_summary_graph(_: int) -> Figure:
    return viz_star_network()
