import datetime
from typing import Any

import dash
import dash_bootstrap_components as dbc
import pandas as pd
import pytz
from dash import Input, Output, callback, dcc, html
from plotly.graph_objs import Figure

from ampere.common import get_db_con
from ampere.styling import AmperePalette, ScreenWidth
from ampere.viz import viz_summary

dash.register_page(__name__, name="summary", path="/", top_nav=True, order=0)


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
    [
        Output("summary-date-slider", "min"),
        Output("summary-date-slider", "value"),
        Output("summary-date-slider", "marks"),
    ],
    [
        Input("summary-df", "data"),
    ],
)
def get_summary_date_ranges(
    df_data: list[dict],
) -> tuple[int, list[int], dict[Any, dict[str, Any]]]:
    df = pd.DataFrame(df_data)
    df["metric_date"] = pd.to_datetime(df["metric_date"], utc=True)
    min_timestamp = df["metric_date"].min().timestamp()
    max_timestamp = df["metric_date"].max().timestamp()

    min_timestamp_ymd = datetime.datetime.fromtimestamp(min_timestamp).strftime(
        "%Y-%m-%d"
    )
    max_timestamp_ymd = datetime.datetime.fromtimestamp(max_timestamp).strftime(
        "%Y-%m-%d"
    )

    date_slider_min = min_timestamp
    date_slider_value = [min_timestamp, max_timestamp]
    date_slider_marks = {
        min_timestamp: {"label": min_timestamp_ymd, "style": {"fontSize": 0}},
        max_timestamp: {"label": max_timestamp_ymd, "style": {"fontSize": 0}},
    }

    return date_slider_min, date_slider_value, date_slider_marks


def get_summary_data() -> list[dict[Any, Any]]:
    con = get_db_con()
    df = con.sql(
        """
    select
        repo_id,
        metric_type,
        metric_date,
        metric_count,
        repo_name
    from main.mart_repo_summary
    order by metric_date
    """
    ).to_df()

    return df.to_dict("records")


@callback(
    [
        Output("summary-graph", "figure"),
        Output("summary-graph", "config"),
    ],
    [
        Input("summary-df", "data"),
        Input("summary-date-slider", "value"),
        Input("breakpoints", "widthBreakpoint"),
    ],
)
def show_summary_graph(
    df_data: list[dict[Any, Any]],
    date_range,
    breakpoint_name: str,
) -> tuple[Figure, dict[str, bool]]:
    df = pd.DataFrame(df_data)
    filter_date_min = datetime.datetime.fromtimestamp(
        date_range[0], tz=pytz.timezone("America/New_York")
    )
    filter_date_max = datetime.datetime.fromtimestamp(
        date_range[1], tz=pytz.timezone("America/New_York")
    )

    df_filtered = df.query(f"metric_date >= '{filter_date_min}'").query(
        f"metric_date <= '{filter_date_max}'"
    )

    fig = viz_summary(df_filtered, screen_width=ScreenWidth(breakpoint_name))

    config = {"displayModeBar": breakpoint_name != "sm"}
    return fig, config


date_slider_step_seconds = 60 * 60 * 24 * 7
layout = [
    dcc.Store("summary-df", data=get_summary_data()),
    dbc.Row(
        [
            dbc.Col(
                html.Div(
                    dcc.RangeSlider(
                        id="summary-date-slider",
                        step=date_slider_step_seconds,
                        allowCross=False,
                        tooltip={
                            "placement": "bottom",
                            "always_visible": True,
                            "transform": "secondsToYMD",
                            "style": {
                                "background": AmperePalette.PAGE_ACCENT_COLOR2,
                                "color": AmperePalette.BRAND_TEXT_COLOR_MUTED,
                                "fontSize": "16px",
                                "paddingLeft": "4px",
                                "paddingRight": "4px",
                                "borderRadius": "10px",
                            },
                        },
                    ),
                    style={"whiteSpace": "nowrap", "paddingLeft": "5%"},
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
    dcc.Loading(
        id="loading-graph",
        type="default",
        children=[dcc.Graph(id="summary-graph")],
    ),
]
