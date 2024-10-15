import dash
import dash_breakpoints
from dash import Input, Output, callback, dcc
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
    dash_breakpoints.WindowBreakpoints(id="breakpoints"),
    dcc.Loading(
        id="loading-graph",
        type="default",
        children=[dcc.Graph(id="summary-graph")],
    ),
]


@callback(Output("summary-graph", "style"), Input("breakpoints", "width"))
def handle_summary_widescreen(display_width_px: int):
    is_widescreen = display_width_px > 1920

    if is_widescreen:
        return {
            "marginTop": "2vw",
            "marginLeft": "20vw",
            "marginRight": "20vw",
        }

    return {
        "marginTop": "2vw",
        "marginLeft": "0vw",
        "marginRight": "0vw",
    }


@callback(
    Output("summary-graph", "figure"),
    [Input("load-interval", "n_intervals"), Input("breakpoints", "width")],
)
def show_summary_graph(_: int, screen_width_px: int) -> Figure:
    print(screen_width_px)
    return viz_summary(show_fig=False, screen_width_px=screen_width_px)
