import dash
from dash import callback, Output, Input
from dash import dcc
from dash import html

from ampere.viz import viz_summary

dash.register_page(__name__, name="summary", path="/", top_nav=True, order=0)

layout = [
    html.H1(children="Summary", style={"textAlign": "center"}),
    # dummy input for reload on refresh
    dcc.Interval(
        id="load-interval",
        n_intervals=0,
        max_intervals=0,
        interval=1,
    ),
    dcc.Graph(id="summary-graph"),
]


@callback(
    Output("summary-graph", "figure"),
    Input("load-interval", "n_intervals"),
)
def show_summary_graph(metric_selection: str):
    print(metric_selection)
    return viz_summary()
