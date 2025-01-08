import datetime
from typing import Any, Optional

import dash
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import pytz
from dash import Input, Output, callback, dcc, html
from plotly.graph_objects import Figure

from ampere.app_shared import cache
from ampere.common import get_frontend_db_con
from ampere.styling import AmperePalette

dash.register_page(__name__, name="downloads", top_nav=True, order=1)

@cache.memoize()
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
        facet_col="group_name",
        template="simple_white",
        category_orders={"group_value": categories},
    )
    fig.for_each_annotation(
        lambda a: a.update(
            text="<b>"
            + a.text.split("=")[-1]
            .replace("_", " ")
            .replace("system release", "cloud platform")
            + "</b>",
            font_size=18,
            bgcolor=AmperePalette.PAGE_ACCENT_COLOR2,
            font_color="white",
            borderpad=5,
        )
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

    fig.update_layout(
        title={
            "y": 0.95,
            "x": 0.5,
            "xanchor": "center",
            "yanchor": "top",
        },
        margin=dict(t=50, l=0, r=0),
        legend=dict(title=None, itemsizing="constant", font=dict(size=14)),
        legend_title_text="",
    )

    return fig


@cache.memoize()
def get_valid_repos() -> list[str]:
    with get_frontend_db_con() as con:
        repos = (
            con.sql(
                """
                select distinct a.repo 
                from mart_downloads_summary a 
                left join stg_repos b on a.repo = b.repo_name
                order by b.stargazers_count desc, a.repo
                """
            )
            .to_df()
            .squeeze()
            .tolist()
        )

    return repos


@callback(
    Output("downloads-overall", "figure"),
    [
        Input("downloads-df", "data"),
        Input("date-slider", "value"),
    ],
)
def viz_downloads_overall(df_data: list[dict], date_range: list[int]):
    df = pd.DataFrame(df_data)
    fig = viz_area(df, "overall", date_range)
    return fig


@callback(
    Output("downloads-package-version", "figure"),
    [
        Input("downloads-df", "data"),
        Input("date-slider", "value"),
    ],
)
def viz_downloads_by_package_version(df_data: list[dict], date_range: list[int]):
    df = pd.DataFrame(df_data)
    fig = viz_area(df, "package_version", date_range)
    return fig


@callback(
    Output("downloads-python-version", "figure"),
    [
        Input("downloads-df", "data"),
        Input("date-slider", "value"),
    ],
)
def viz_downloads_by_python_version(df_data: list[dict], date_range: list[int]):
    df = pd.DataFrame(df_data)
    fig = viz_area(df, "python_version", date_range)
    return fig


@callback(
    [
        Output("downloads-cloud", "figure"),
        Output("downloads-fade", "is_in"),
    ],
    [
        Input("downloads-df", "data"),
        Input("date-slider", "value"),
    ],
)
def viz_downloads_by_cloud_provider(df_data: list[dict], date_range: list[int]):
    df = pd.DataFrame(df_data)
    fig = viz_area(df, "system_release", date_range)
    return fig, True


@callback(
    Output("downloads-df", "data"),
    Input("repo-selection", "value"),
)
@cache.memoize()
def get_downloads_summary(repo_name: str) -> list[dict]:
    print(f"cache miss: computing get_downloads_summary for {repo_name}")

    with get_frontend_db_con() as con:
        df = con.sql(
            f"""
            select
            repo,
            download_date,
            group_name,
            group_value,
            download_count
            from mart_downloads_summary
            where repo = '{repo_name}'
            order by download_date, download_count
            """,
        ).to_df()

    return df.to_dict("records")


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
            "color": AmperePalette.BRAND_TEXT_COLOR,
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


def layout():
    return [
        dcc.Store("downloads-df"),
        dbc.Fade(
            id="downloads-fade",
            children=[
                dbc.Row(
                    children=[
                        dbc.Col(
                            dcc.Dropdown(
                                get_valid_repos(),
                                placeholder="quinn",
                                value="quinn",
                                clearable=False,
                                searchable=False,
                                id="repo-selection",
                                style={
                                    "background": AmperePalette.PAGE_ACCENT_COLOR2,
                                    "border": AmperePalette.PAGE_ACCENT_COLOR2,
                                    "borderRadius": "10px",
                                    "fontSize": "20px",
                                    "marginRight": "10%",
                                    "marginTop": "2%",
                                    "paddingBottom": "2px",
                                    "paddingTop": "2px",
                                },
                            ),
                            width=1,
                        ),
                        dbc.Col(
                            html.Div(
                                dcc.RangeSlider(
                                    id="date-slider",
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
                dcc.Graph("downloads-overall"),
                dcc.Graph("downloads-package-version"),
                dcc.Graph("downloads-python-version"),
                dcc.Graph("downloads-cloud"),
            ],
            style={"transition": "opacity 300ms ease-in"},
            is_in=False,
        ),
    ]
