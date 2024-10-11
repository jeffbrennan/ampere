import dash
import dash_bootstrap_components as dbc
from dash import dcc, callback, Output, Input
import dash_breakpoints
from plotly.graph_objects import Figure

from ampere.pages.side_bar import sidebar
from ampere.viz import viz_star_network

dash.register_page(__name__, name="network", top_nav=True, order=1)


def layout(**kwargs):
    return dbc.Row(
        [
            dbc.Col(sidebar(), width=2),
            dcc.Interval(
                id="network-stargazer-load-interval",
                n_intervals=0,
                max_intervals=0,
                interval=1,
            ),
            dash_breakpoints.WindowBreakpoints(id="network-stargazer-breakpoints"),
            dbc.Col(dcc.Loading(dcc.Graph(id="network-stargazer-graph")), width=10),
        ]
    )


@callback(
    Output("network-stargazer-graph", "style"),
    Input("network-stargazer-breakpoints", "width"),
)
def handle_stargazer_widescreen(display_width_px: int):
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
    Output("network-stargazer-graph", "figure"),
    Input("network-stargazer-load-interval", "n_intervals"),
)
def show_summary_graph(_: int) -> Figure:
    return viz_star_network()
