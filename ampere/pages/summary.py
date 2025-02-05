import os
from typing import Any

import dash_bootstrap_components as dbc
import pandas as pd
from dash import Input, Output, callback, dcc, html
from plotly.graph_objs import Figure

from ampere.app_shared import cache, update_tooltip
from ampere.common import timeit
from ampere.styling import ScreenWidth
from ampere.viz import (
    get_summary_data,
    read_plotly_fig_pickle,
    viz_summary,
)


@callback(
    Output("summary-date-slider", "tooltip"),
    Input("breakpoints", "widthBreakpoint"),
)
def update_summary_slider(breakpoint_name: str) -> dict[Any, Any]:
    return update_tooltip(breakpoint_name)


@callback(
    [
        Output("summary-date-slider", "min"),
        Output("summary-date-slider", "max"),
        Output("summary-date-slider", "value"),
        Output("summary-date-slider", "marks"),
        Output("summary-date-bounds", "data"),
    ],
    [
        Input("summary-df", "data"),
    ],
)
def get_summary_date_ranges(
    df_data: list[dict],
) -> tuple[int, int, list[int], dict[Any, dict[str, Any]], list[int]]:
    df = pd.DataFrame(df_data)
    df["metric_date"] = pd.to_datetime(df["metric_date"], utc=True)
    min_date_dt = df["metric_date"].min()
    min_date_seconds = min_date_dt.timestamp()

    max_date_dt = df["metric_date"].max()
    max_date_seconds = max_date_dt.timestamp()

    date_slider_marks = {
        min_date_seconds: {
            "label": min_date_dt.strftime("%Y-%m-%d"),
            "style": {"fontSize": 0},
        },
        max_date_seconds: {
            "label": max_date_dt.strftime("%Y-%m-%d"),
            "style": {"fontSize": 0},
        },
    }

    return (
        min_date_seconds,
        max_date_seconds,
        [min_date_seconds, max_date_seconds],
        date_slider_marks,
        [min_date_seconds, max_date_seconds],
    )


@cache.memoize()
@timeit
def get_summary_records() -> list[dict[Any, Any]]:
    return get_summary_data().to_dict("records")


@timeit
def get_viz_summary(
    df_data: list[dict],
    breakpoint_name: str,
    date_range: list[int],
    dark_mode: bool,
    date_bounds: list[int],
    metric_type: str,
) -> tuple[Figure, dict]:
    env = os.getenv("AMPERE_ENV")
    if date_range == date_bounds and env == "prod":
        mode = "dark" if dark_mode else "light"
        f_name = f"summary_{metric_type}_{mode}_{breakpoint_name}"
        try:
            fig = read_plotly_fig_pickle(f_name)
            print(f"obtained {metric_type} fig from cache")
            return fig, {}

        except Exception as e:
            print(e)
            pass

    fig = viz_summary(
        df=pd.DataFrame(df_data),
        metric_type=metric_type,
        date_range=date_range,
        screen_width=ScreenWidth(breakpoint_name),
        dark_mode=dark_mode,
    )

    return fig, {}


@callback(
    [
        Output("summary-stars", "figure"),
        Output("summary-stars", "style"),
    ],
    [
        Input("summary-df", "data"),
        Input("breakpoints", "widthBreakpoint"),
        Input("summary-date-slider", "value"),
        Input("color-mode-switch", "value"),
        Input("summary-date-bounds", "data"),
    ],
)
@timeit
def viz_summary_stars(
    df_data: list[dict],
    breakpoint_name: str,
    date_range: list[int],
    dark_mode: bool,
    date_bounds: list[int],
):
    return get_viz_summary(
        df_data, breakpoint_name, date_range, dark_mode, date_bounds, "stars"
    )


@callback(
    [
        Output("summary-issues", "figure"),
        Output("summary-issues", "style"),
    ],
    [
        Input("summary-df", "data"),
        Input("breakpoints", "widthBreakpoint"),
        Input("summary-date-slider", "value"),
        Input("color-mode-switch", "value"),
        Input("summary-date-bounds", "data"),
    ],
)
def viz_summary_issues(
    df_data: list[dict],
    breakpoint_name: str,
    date_range: list[int],
    dark_mode: bool,
    date_bounds: list[int],
):
    return get_viz_summary(
        df_data, breakpoint_name, date_range, dark_mode, date_bounds, "issues"
    )


@callback(
    [
        Output("summary-commits", "figure"),
        Output("summary-commits", "style"),
    ],
    [
        Input("summary-df", "data"),
        Input("breakpoints", "widthBreakpoint"),
        Input("summary-date-slider", "value"),
        Input("color-mode-switch", "value"),
        Input("summary-date-bounds", "data"),
    ],
)
def viz_summary_commits(
    df_data: list[dict],
    breakpoint_name: str,
    date_range: list[int],
    dark_mode: bool,
    date_bounds: list[int],
):
    return get_viz_summary(
        df_data, breakpoint_name, date_range, dark_mode, date_bounds, "commits"
    )


@callback(
    Output("summary-graph-fade", "is_in"),
    [
        Input("summary-stars", "figure"),
        Input("summary-issues", "figure"),
        Input("summary-commits", "figure"),
    ],
)
def update_summary_graph_fade(fig1, fig2, fig3):
    return all([fig1, fig2, fig3])


@callback(
    [
        Output("date-filter-width", "width"),
        Output("filter-padding-width", "width"),
        Output("filter-row", "style"),
    ],
    Input("breakpoints", "widthBreakpoint"),
)
def update_filter_for_mobile(breakpoint_name: str):
    filter_style = {"top": "60px"}
    if breakpoint_name in [ScreenWidth.xs, ScreenWidth.sm]:
        filter_style.update({"paddingTop": "20px"})
        return 11, 1, filter_style

    return 3, 8, filter_style


def layout():
    return [
        dcc.Store("summary-df", data=get_summary_records()),
        dcc.Store("summary-date-bounds"),
        dbc.Fade(
            id="summary-graph-fade",
            children=[
                dbc.Row(
                    [
                        dbc.Col(
                            html.Div(
                                dcc.RangeSlider(
                                    id="summary-date-slider",
                                    step=86400,  # daily
                                    allowCross=False,
                                ),
                                style={
                                    "whiteSpace": "nowrap",
                                    "paddingLeft": "5%",
                                    "marginTop": "6px",
                                },
                            ),
                            width=3,
                            id="date-filter-width",
                            style={"marginLeft": "5%"},
                        ),
                        dbc.Col(width=8, id="filter-padding-width"),
                    ],
                    style={"z-index": "100", "top": "60px"},
                    id="filter-row",
                ),
                html.Br(),
                dcc.Graph(
                    "summary-stars",
                    style={"visibility": "hidden"},
                    config={"displayModeBar": False},
                ),
                dcc.Graph(
                    "summary-issues",
                    style={"visibility": "hidden"},
                    config={"displayModeBar": False},
                ),
                dcc.Graph(
                    "summary-commits",
                    style={"visibility": "hidden"},
                    config={"displayModeBar": False},
                ),
            ],
            style={"transition": "opacity 200ms ease-in", "minHeight": "100vh"},
            is_in=False,
        ),
    ]
