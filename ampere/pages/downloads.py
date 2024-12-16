import datetime
import time
from typing import Any, Optional

import dash
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import pytz
from dash import Input, Output, callback, dcc, html
from plotly.graph_objects import Figure

from ampere.common import get_db_con
from ampere.styling import AmperePalette

dash.register_page(__name__, name="downloads", top_nav=True, order=1)


def create_downloads_summary() -> pd.DataFrame:
    con = get_db_con()
    return con.sql(
        """
            select
            repo,
            download_date,
            group_name,
            group_value,
            download_count
            from mart_downloads_summary
            where group_name <> 'system_name'
            order by download_date, download_count
            """
    ).to_df()


def viz_line(df: pd.DataFrame, group_name: str) -> Figure:
    df_filtered = df.query(f"group_name=='{group_name}'")
    print(df_filtered.shape)
    fig = px.line(
        df_filtered,
        x="download_date",
        y="download_count",
        color="group_value",
        title=group_name,
        template="simple_white",
    )
    fig.for_each_yaxis(
        lambda y: y.update(
            title="",
            showline=True,
            linewidth=1,
            linecolor="black",
            mirror=True,
            tickfont_size=14,
        )
    )
    fig.for_each_xaxis(
        lambda x: x.update(
            title="",
            showline=True,
            linewidth=1,
            linecolor="black",
            mirror=True,
            showticklabels=True,
            tickfont_size=14,
        )
    )
    fig.update_yaxes(matches=None, showticklabels=True)

    fig.update_layout(margin=dict(l=0, r=0))
    return fig


def viz_area(
    df: pd.DataFrame,
    group_name: str,
    date_range: Optional[list[int]] = None,
) -> Figure:
    df_filtered = df.query(f"group_name=='{group_name}'")
    if date_range is not None:
        filter_date_min = datetime.datetime.fromtimestamp(
            date_range[0], tz=pytz.timezone("America/New_York")
        )
        filter_date_max = datetime.datetime.fromtimestamp(
            date_range[1], tz=pytz.timezone("America/New_York")
        )

        df_filtered = df_filtered.query(f"download_date >= '{filter_date_min}'").query(
            f"download_date <= '{filter_date_max}'"
        )

    max_date = df_filtered["download_date"].max()
    categories = (
        df_filtered[(df_filtered["download_date"] == max_date)]
        .sort_values("download_count", ascending=False)["group_value"]
        .tolist()
    )

    fig = px.area(
        df_filtered,
        x="download_date",
        y="download_count",
        color="group_value",
        title=f"<b>{group_name.replace('_', ' ')}</b>",
        template="simple_white",
        category_orders={"group_value": categories},
    )
    fig.for_each_yaxis(
        lambda y: y.update(
            title="",
            showline=True,
            linewidth=1,
            linecolor="black",
            mirror=True,
            tickfont_size=14,
        )
    )
    fig.for_each_xaxis(
        lambda x: x.update(
            title="",
            showline=True,
            linewidth=1,
            linecolor="black",
            mirror=True,
            showticklabels=True,
            tickfont_size=14,
        )
    )
    fig.update_yaxes(matches=None, showticklabels=True)
    fig.update_layout(margin=dict(l=0, r=0))

    fig.update_layout(
        title={
            "y": 0.95,  # Adjust this value to move the title closer or further
            "x": 0.5,  # Center the title horizontally
            "xanchor": "center",
            "yanchor": "top",
        },
        margin=dict(t=50),  # Adjust top margin to avoid overlap with title
    )
    if len(categories) == 1:
        fig.update_layout(showlegend=False)
    else:
        fig.update_layout(legend_title_text="")

    return fig


def get_valid_repos() -> list[str]:
    con = get_db_con()
    result = con.sql("select distinct repo from mart_downloads_summary").to_df()
    return result.squeeze().tolist()


@callback(
    [
        Output("downloads-overall", "figure"),
        Output("downloads-overall", "style"),
    ],
    [
        Input("downloads-df", "data"),
        Input("breakpoints", "widthBreakpoint"),
        Input("date-slider", "value"),
    ],
)
def viz_downloads_overall(
    df_data: list[dict], breakpoint_name: str, date_range: list[int]
) -> tuple[Figure, dict]:
    df = pd.DataFrame(df_data)
    fig = viz_area(df, "overall", date_range)
    return fig, {}


@callback(
    [
        Output("downloads-cloud", "figure"),
        Output("downloads-cloud", "style"),
    ],
    [
        Input("downloads-df", "data"),
        Input("breakpoints", "widthBreakpoint"),
        Input("date-slider", "value"),
    ],
)
def viz_downloads_by_cloud_provider(
    df_data: list[dict], breakpoint_name: str, date_range: list[int]
) -> tuple[Figure, dict]:
    df = pd.DataFrame(df_data)
    fig = viz_area(df, "system_release", date_range)
    return fig, {}


@callback(
    [
        Output("downloads-python-version", "figure"),
        Output("downloads-python-version", "style"),
    ],
    [
        Input("downloads-df", "data"),
        Input("breakpoints", "widthBreakpoint"),
        Input("date-slider", "value"),
    ],
)
def viz_downloads_by_python_version(
    df_data: list[dict], breakpoint_name: str, date_range: list[int]
) -> tuple[Figure, dict]:
    time.sleep(0.1)
    df = pd.DataFrame(df_data)
    fig = viz_area(df, "python_version", date_range)
    return fig, {}


@callback(
    [
        Output("downloads-package-version", "figure"),
        Output("downloads-package-version", "style"),
    ],
    [
        Input("downloads-df", "data"),
        Input("breakpoints", "widthBreakpoint"),
        Input("date-slider", "value"),
    ],
)
def viz_downloads_by_package_version(
    df_data: list[dict], breakpoint_name: str, date_range: list[int]
) -> tuple[Figure, dict]:
    df = pd.DataFrame(df_data)
    fig = viz_area(df, "package_version", date_range)
    return fig, {}


@callback(
    Output("downloads-df", "data"),
    Input("repo-selection", "value"),
)
def get_downloads_summary(repo_name: str) -> list[dict]:
    df = create_downloads_summary()
    return df.query(f"repo=='{repo_name}'").to_dict("records")


@callback(
    Output("date-slider", "tooltip"),
    [
        Input("date-slider", "min"),
        Input("date-slider", "max"),
        Input("date-slider", "value"),
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
            "color": AmperePalette.BRAND_TEXT_COLOR_MUTED,
            "fontSize": "16px",
            "paddingLeft": "4px",
            "paddingRight": "4px",
            "borderRadius": "10px",
        },
    }


@callback(
    [
        Output("date-slider", "min"),
        Output("date-slider", "max"),
        Output("date-slider", "value"),
        Output("date-slider", "marks"),
    ],
    [
        Input("downloads-df", "data"),
    ],
)
def get_downloads_summary_date_ranges(
    df_data: list[dict],
) -> tuple[int, int, list[int], dict[Any, dict[str, Any]]]:
    df = pd.DataFrame(df_data)
    df["download_date"] = pd.to_datetime(df["download_date"], utc=True)
    min_timestamp = df["download_date"].min().timestamp()
    max_timestamp = df["download_date"].max().timestamp()

    min_timestamp_ymd = datetime.datetime.fromtimestamp(min_timestamp).strftime(
        "%Y-%m-%d"
    )
    max_timestamp_ymd = datetime.datetime.fromtimestamp(max_timestamp).strftime(
        "%Y-%m-%d"
    )

    date_slider_value = [min_timestamp, max_timestamp]
    date_slider_marks = {
        min_timestamp: {"label": min_timestamp_ymd, "style": {"fontSize": 0}},
        max_timestamp: {"label": max_timestamp_ymd, "style": {"fontSize": 0}},
    }
    return (
        min_timestamp,
        max_timestamp,
        date_slider_value,
        date_slider_marks,
    )


date_slider_step_seconds = 60 * 60 * 24 * 7

layout = [
    html.Br(),
    dcc.Store("downloads-df"),
    dbc.Row(
        children=[
            dbc.Col(
                dcc.Dropdown(
                    get_valid_repos(),
                    placeholder="quinn",
                    value="quinn",
                    clearable=False,
                    id="repo-selection",
                    style={
                        "background": AmperePalette.PAGE_ACCENT_COLOR2,
                        "border": AmperePalette.PAGE_ACCENT_COLOR2,
                        "borderRadius": "10px",
                        "fontSize": "20px",
                        "marginRight": "10%",
                        "paddingTop": "2px",
                        "paddingBottom": "2px",
                    },
                ),
                width=1,
            ),
            dbc.Col(
                html.Div(
                    dcc.RangeSlider(
                        id="date-slider",
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
    dcc.Graph("downloads-overall", style={"visibility": "hidden"}),
    dcc.Graph("downloads-package-version", style={"visibility": "hidden"}),
    dcc.Graph("downloads-python-version", style={"visibility": "hidden"}),
    dcc.Graph("downloads-cloud", style={"visibility": "hidden"}),
]
