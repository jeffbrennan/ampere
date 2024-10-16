import dash
import dash_breakpoints
from dash import Input, Output, State, callback, dcc
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
    dash_breakpoints.WindowBreakpoints(
        id="breakpoints",
        widthBreakpointThresholdsPx=[1200, 1920, 2560],
        widthBreakpointNames=["sm", "md", "lg", "xl"],
    ),
    dcc.Loading(
        id="loading-graph",
        type="default",
        children=[dcc.Graph(id="summary-graph")],
    ),
]


@callback(Output("summary-graph", "style"), Input("breakpoints", "widthBreakpoint"))
def handle_summary_widescreen(breakpoint_name: str):
    is_widescreen = breakpoint_name == "lg"

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
    [Input("load-interval", "n_intervals"), Input("breakpoints", "widthBreakpoint")],
)
def show_summary_graph(_: int, breakpoint_name: str) -> Figure:
    print(breakpoint_name)
    breakpoint_mapping = {"sm": 1199, "md": 1899, "lg": 2559, "xl": 2561}
    return viz_summary(
        show_fig=False, screen_width_px=breakpoint_mapping[breakpoint_name]
    )
