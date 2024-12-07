import datetime
from typing import Optional

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
    repo_name: str,
    group_name: str,
    date_range: Optional[list[int]] = None,
) -> Figure:
    df_filtered = df.query(f"group_name=='{group_name}'").query(f"repo=='{repo_name}'")
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
    Output("downloads-overall", "figure"),
    [
        Input("repo-selection", "value"),
        Input("breakpoints", "widthBreakpoint"),
        Input("date-slider", "value"),
    ],
)
def viz_downloads_overall(
    repo_name: str, breakpoint_name: str, date_range: list[int]
) -> Figure:
    df = create_downloads_summary()
    fig = viz_area(df, repo_name, "overall", date_range)
    return fig


@callback(
    Output("downloads-cloud", "figure"),
    [
        Input("repo-selection", "value"),
        Input("breakpoints", "widthBreakpoint"),
        Input("date-slider", "value"),
    ],
)
def viz_downloads_by_cloud_provider(
    repo_name: str, breakpoint_name: str, date_range: list[int]
) -> Figure:
    df = create_downloads_summary()
    fig = viz_area(df, repo_name, "system_release", date_range)
    return fig


@callback(
    Output("downloads-python-version", "figure"),
    [
        Input("repo-selection", "value"),
        Input("breakpoints", "widthBreakpoint"),
        Input("date-slider", "value"),
    ],
)
def viz_downloads_by_python_version(
    repo_name: str, breakpoint_name: str, date_range: list[int]
) -> Figure:
    df = create_downloads_summary()
    fig = viz_area(df, repo_name, "python_version", date_range)
    return fig


@callback(
    Output("downloads-package-version", "figure"),
    [
        Input("repo-selection", "value"),
        Input("breakpoints", "widthBreakpoint"),
        Input("date-slider", "value"),
    ],
)
def viz_downloads_by_package_version(
    repo_name: str, breakpoint_name: str, date_range: list[int]
) -> Figure:
    df = create_downloads_summary()
    fig = viz_area(df, repo_name, "package_version", date_range)
    return fig


df = create_downloads_summary()
min_timestamp = df["download_date"].min().timestamp()
max_timestamp = df["download_date"].max().timestamp()

min_timestamp_ymd = datetime.datetime.fromtimestamp(min_timestamp).strftime("%Y-%m-%d")
max_timestamp_ymd = datetime.datetime.fromtimestamp(max_timestamp).strftime("%Y-%m-%d")

date_slider_step_seconds = 60 * 60 * 24 * 7

layout = [
    html.Br(),
    dbc.Row(
        [
            dbc.Col(
                dcc.Dropdown(
                    get_valid_repos(),
                    placeholder="quinn",
                    value="quinn",
                    clearable=False,
                    id="repo-selection",
                ),
                width=2,
            ),
            dbc.Col(
                html.Div(
                    dcc.RangeSlider(
                        id="date-slider",
                        min=min_timestamp,
                        value=[min_timestamp, max_timestamp],
                        step=date_slider_step_seconds,
                        marks={
                            min_timestamp: {
                                "label": min_timestamp_ymd,
                                "style": {"fontSize": 0},
                            },
                            max_timestamp: {
                                "label": max_timestamp_ymd,
                                "style": {"fontSize": 0},
                            },
                        },
                        allowCross=False,
                        tooltip={
                            "placement": "bottom",
                            "always_visible": True,
                            "transform": "secondsToYMD",
                            "style": {
                                "background": AmperePalette.PAGE_ACCENT_COLOR,
                                "border": AmperePalette.PAGE_ACCENT_COLOR,
                            },
                        },
                    ),
                    style={"whiteSpace": "nowrap"},
                ),
                width=3,
            ),
            dbc.Col(width=7),
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
        children=[
            dcc.Graph("downloads-overall"),
            dcc.Graph("downloads-package-version"),
            dcc.Graph("downloads-python-version"),
            dcc.Graph("downloads-cloud"),
        ],
    ),
]
