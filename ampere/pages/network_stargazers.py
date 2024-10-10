import dash
import dash_bootstrap_components as dbc
from dash import dcc, callback, Output, Input
from plotly.graph_objects import Figure

from ampere.pages.side_bar import sidebar
from ampere.viz import viz_star_network

dash.register_page(__name__, name="network", top_nav=True, order=1)


def layout(**kwargs):
    return dbc.Row(
        [
            dbc.Col(sidebar(), width=1),
            dcc.Interval(
                id="network-stargazer-load-interval",
                n_intervals=0,
                max_intervals=0,
                interval=1,
            ),
            dbc.Col(
                dcc.Graph(
                    id="network-stargazer-graph",
                    style={"height": "95vh"},
                ),
                width=11,
            ),
        ]
    )


@callback(
    Output("network-stargazer-graph", "figure"),
    Input("network-stargazer-load-interval", "n_intervals"),
)
def show_summary_graph(_: int) -> Figure:
    return viz_star_network()
