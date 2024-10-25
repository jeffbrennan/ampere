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
        widthBreakpointThresholdsPx=[
            1200,
            1920,
            2560,
        ],
        widthBreakpointNames=["sm", "md", "lg", "xl"],
    ),
    dcc.Loading(
        id="loading-graph",
        type="default",
        children=[dcc.Graph(id="summary-graph")],
    ),
]


@callback(Output("summary-graph", "style"), Input("breakpoints", "widthBreakpoint"))
def handle_summary_sizes(breakpoint_name: str):
    side_margins = {"sm": 0, "md": 2, "lg": 4, "xl": 6}
    y_axis_width_adjustment_vw = 4

    return {
        "marginTop": "2vw",
        "marginLeft": f"{side_margins[breakpoint_name]}vw",
        "marginRight": f"{side_margins[breakpoint_name] + y_axis_width_adjustment_vw}vw",
    }


@callback(
    [Output("summary-graph", "figure"), Output("summary-graph", "config")],
    [Input("load-interval", "n_intervals"), Input("breakpoints", "widthBreakpoint")],
)
def show_summary_graph(_: int, breakpoint_name: str) -> tuple[Figure, dict[str, bool]]:
    breakpoint_mapping = {"sm": 1199, "md": 1899, "lg": 2559, "xl": 2561}
    fig = viz_summary(
        show_fig=False, screen_width_px=breakpoint_mapping[breakpoint_name]
    )

    config = {"displayModeBar": breakpoint_name != "sm"}
    return fig, config
