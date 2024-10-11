import dash
from dash import callback, Output, Input
from dash import dcc
from plotly.graph_objs import Figure

from ampere.viz import viz_summary

dash.register_page(__name__, name="summary", path="/", top_nav=True, order=0)

layout = [
    # dummy input for reload on refresh
    dcc.Interval(
        id="load-interval",
        n_intervals=0,
        max_intervals=0,
        interval=1,
    ),
    dcc.Loading(
        id="loading-graph",
        type="default",
        children=[
            dcc.Graph(id="summary-graph"),
        ],
    ),
]


@callback(
    Output("summary-graph", "figure"),
    Input("load-interval", "n_intervals"),
)
def show_summary_graph(_: int) -> Figure:
    return viz_summary()
