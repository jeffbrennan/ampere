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

    return (  # pyright: ignore [reportReturnType]
        min_date_seconds,
        max_date_seconds,
        [min_date_seconds, max_date_seconds],
        date_slider_marks,
    )


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
        Output("summary-graph", "style"),
        Output("summary-graph-fade", "is_in"),
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
) -> tuple[Figure, dict[str, bool], dict, bool]:
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
    return fig, config, {}, True


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
    dbc.Fade(
        id="summary-graph-fade",
        children=dcc.Graph(
            id="summary-graph",
            style={"visibility": "hidden"},
        ),
        style={"transition": "opacity 1000ms ease"},
        is_in=False,
    ),
]
