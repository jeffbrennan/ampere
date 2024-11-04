import dash
from dash import Input, Output, callback, dcc
from plotly.graph_objs import Figure

from ampere.styling import ScreenWidth
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
        children=[dcc.Graph(id="summary-graph")],
    ),
]


@callback(Output("summary-graph", "style"), Input("breakpoints", "widthBreakpoint"))
def handle_summary_sizes(breakpoint_name: str):
    side_margins = {"xs": 0, "sm": 0, "md": 2, "lg": 4, "xl": 6}
    y_axis_width_adjustment_vw = 0

    if breakpoint_name in ["xs", "sm"]:
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
    fig = viz_summary(show_fig=False, screen_width=ScreenWidth(breakpoint_name))

    config = {"displayModeBar": breakpoint_name != "sm"}
    return fig, config
