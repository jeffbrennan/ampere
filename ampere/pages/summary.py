from typing import Any

import dash
import dash_bootstrap_components as dbc
import pandas as pd
from dash import Input, Output, callback, dcc, html

from ampere.app_shared import cache
from ampere.common import get_frontend_db_con
from ampere.styling import AmperePalette, ScreenWidth
from ampere.viz import viz_summary

dash.register_page(__name__, name="summary", path="/", top_nav=True, order=0)


@callback(
    Output("summary-date-slider", "tooltip"),
    [
        Input("summary-date-slider", "min"),
        Input("summary-date-slider", "max"),
        Input("summary-date-slider", "value"),
    ],
)
def toggle_slider_tooltip_visibility(
    min_date_seconds: int, max_date_seconds: int, date_range: list[int]
) -> dict[Any, Any]:
    always_visible = (
        date_range[0] == min_date_seconds and date_range[1] == max_date_seconds
    )
    return {
        "placement": "bottom",
        "always_visible": always_visible,
        "transform": "secondsToYMD",
        "style": {
            "background": AmperePalette.PAGE_ACCENT_COLOR2,
            "color": AmperePalette.BRAND_TEXT_COLOR,
            "fontSize": "16px",
            "paddingLeft": "4px",
            "paddingRight": "4px",
            "borderRadius": "10px",
        },
    }


@callback(
    [
        Output("summary-date-slider", "min"),
        Output("summary-date-slider", "max"),
        Output("summary-date-slider", "value"),
        Output("summary-date-slider", "marks"),
    ],
    [
        Input("summary-df", "data"),
    ],
)
def get_summary_date_ranges(
    df_data: list[dict],
) -> tuple[int, int, list[int], dict[Any, dict[str, Any]]]:
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
    )


@cache.memoize()
def get_summary_data() -> list[dict[Any, Any]]:
    with get_frontend_db_con() as con:
        df = con.sql(
            """
        select
            repo_name,
            metric_type,
            metric_date,
            metric_count,
        from main.mart_repo_summary
        order by metric_date
    """,
        ).to_df()

    return df.to_dict("records")


@callback(
    [
        Output("summary-stars", "figure"),
        Output("summary-graph-fade", "is_in"),
    ],
    [
        Input("summary-df", "data"),
        Input("breakpoints", "widthBreakpoint"),
        Input("summary-date-slider", "value"),
    ],
)
def viz_summary_stars(df_data: list[dict], breakpoint_name: str, date_range: list[int]):
    df = pd.DataFrame(df_data)
    fig = viz_summary(
        df=df,
        metric_type="stars",
        date_range=date_range,
        screen_width=ScreenWidth(breakpoint_name),
    )
    return fig, True


@callback(
    Output("summary-issues", "figure"),
    [
        Input("summary-df", "data"),
        Input("breakpoints", "widthBreakpoint"),
        Input("summary-date-slider", "value"),
    ],
)
def viz_summary_issues(df_data: list[dict], breakpoint_name: str, date_range: list[int]):
    df = pd.DataFrame(df_data)
    return viz_summary(
        df=df,
        metric_type="issues",
        date_range=date_range,
        screen_width=ScreenWidth(breakpoint_name),
    )


@callback(
    Output("summary-commits", "figure"),
    [
        Input("summary-df", "data"),
        Input("breakpoints", "widthBreakpoint"),
        Input("summary-date-slider", "value"),
    ],
)
def viz_summary_commits(df_data: list[dict], breakpoint_name: str, date_range: list[int]):
    df = pd.DataFrame(df_data)
    return viz_summary(
        df=df,
        metric_type="commits",
        date_range=date_range,
        screen_width=ScreenWidth(breakpoint_name),
    )


def layout():
    return [
        dcc.Store("summary-df", data=get_summary_data()),
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
                                },
                            ),
                            width=3,
                        ),
                        dbc.Col(width=8),
                    ],
                    style={
                        "position": "sticky",
                        "z-index": "100",
                        "top": "60px",
                    },
                ),
                dcc.Graph("summary-stars"),
                dcc.Graph("summary-issues"),
                dcc.Graph("summary-commits"),
            ],
            style={"transition": "opacity 500ms ease-in"},
            is_in=False,
        ),
    ]
