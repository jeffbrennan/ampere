import datetime
from typing import Any

import dash_bootstrap_components as dbc
import pandas as pd
from dash import Input, Output, callback, dcc, html
from plotly.graph_objects import Figure

from ampere.app_shared import cache
from ampere.common import timeit
from ampere.styling import AmperePalette
from ampere.viz import (
    get_repos_with_downloads,
    read_dataframe_pickle,
    read_plotly_fig_pickle,
    viz_downloads,
)


@timeit
def get_viz_downloads(
    df_data: list[dict],
    date_range: list[int],
    dark_mode: bool,
    date_bounds: list[int],
    group: str,
    repo: str,
) -> tuple[Figure, dict]:
    if date_range == date_bounds:
        mode = "dark" if dark_mode else "light"
        f_name = f"downloads_{repo}_{group}_{mode}"
        try:
            fig = read_plotly_fig_pickle(f_name)
            print(f"obtained {group} fig from cache")
            return fig, {}

        except Exception as e:
            print(e)
            pass

    fig = viz_downloads(
        pd.DataFrame(df_data),
        group_name=group,
        date_range=date_range,
        dark_mode=dark_mode,
    )

    return fig, {}


@cache.memoize()
@timeit
def dash_get_repos_with_downloads():
    return get_repos_with_downloads()


@callback(
    [
        Output("downloads-overall", "figure"),
        Output("downloads-overall", "style"),
    ],
    [
        Input("downloads-df", "data"),
        Input("date-slider", "value"),
        Input("color-mode-switch", "value"),
        Input("downloads-date-bounds", "data"),
        Input("repo-selection", "value"),
    ],
)
def viz_downloads_overall(
    df_data: list[dict],
    date_range: list[int],
    dark_mode: bool,
    date_bounds: list[int],
    repo_name: str,
):
    return get_viz_downloads(
        df_data=df_data,
        date_range=date_range,
        dark_mode=dark_mode,
        date_bounds=date_bounds,
        group="overall",
        repo=repo_name,
    )


@callback(
    [
        Output("downloads-package-version", "figure"),
        Output("downloads-package-version", "style"),
    ],
    [
        Input("downloads-df", "data"),
        Input("date-slider", "value"),
        Input("color-mode-switch", "value"),
        Input("downloads-date-bounds", "data"),
        Input("repo-selection", "value"),
    ],
)
def viz_downloads_by_package_version(
    df_data: list[dict],
    date_range: list[int],
    dark_mode: bool,
    date_bounds: list[int],
    repo_name: str,
):
    return get_viz_downloads(
        df_data=df_data,
        date_range=date_range,
        dark_mode=dark_mode,
        date_bounds=date_bounds,
        group="package_version",
        repo=repo_name,
    )


@callback(
    [
        Output("downloads-python-version", "figure"),
        Output("downloads-python-version", "style"),
    ],
    [
        Input("downloads-df", "data"),
        Input("date-slider", "value"),
        Input("color-mode-switch", "value"),
        Input("downloads-date-bounds", "data"),
        Input("repo-selection", "value"),
    ],
)
def viz_downloads_by_python_version(
    df_data: list[dict],
    date_range: list[int],
    dark_mode: bool,
    date_bounds: list[int],
    repo_name: str,
):
    return get_viz_downloads(
        df_data=df_data,
        date_range=date_range,
        dark_mode=dark_mode,
        date_bounds=date_bounds,
        group="python_version",
        repo=repo_name,
    )


@callback(
    Output("downloads-fade", "is_in"),
    [
        Input("downloads-overall", "figure"),
        Input("downloads-package-version", "figure"),
        Input("downloads-python-version", "figure"),
    ],
)
def update_downloads_graph_fade(fig1, fig2, fig3):
    return all([fig1, fig2, fig3])


@callback(
    Output("downloads-df", "data"),
    Input("repo-selection", "value"),
)
@cache.memoize()
@timeit
def get_downloads_records(repo_name: str) -> list[dict]:
    print(f"cache miss: computing get_downloads_records for {repo_name}")
    df = read_dataframe_pickle(f"downloads_df_{repo_name}")
    return df.to_dict("records")


@callback(
    Output("date-slider", "tooltip"),
    [
        Input("date-slider", "min"),
        Input("date-slider", "max"),
        Input("date-slider", "value"),
    ],
)
@timeit
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
        Output("downloads-date-bounds", "data"),
    ],
    [
        Input("downloads-df", "data"),
    ],
)
@timeit
def get_downloads_records_date_ranges(df_data: list[dict]):
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
        [min_timestamp, max_timestamp],
    )


@callback(
    [Output("repo-selection", "className"), Output("repo-selection", "style")],
    Input("color-mode-switch", "value"),
)
def update_dropdown_menu_color(dark_mode: bool):
    class_name = "" if dark_mode else "light-mode"
    style = {
        "borderRadius": "10px",
        "fontSize": "20px",
        "marginRight": "10%",
        "marginTop": "2%",
        "paddingBottom": "2px",
        "paddingTop": "2px",
    }
    return class_name, style


def layout():
    return [
        dcc.Store("downloads-df"),
        dcc.Store("downloads-date-bounds"),
        dbc.Fade(
            id="downloads-fade",
            children=[
                dbc.Row(
                    children=[
                        dbc.Col(
                            dcc.Dropdown(
                                dash_get_repos_with_downloads(),
                                placeholder="quinn",
                                value="quinn",
                                clearable=False,
                                searchable=False,
                                id="repo-selection",
                            ),
                            width=2,
                        ),
                        dbc.Col(
                            html.Div(
                                dcc.RangeSlider(
                                    id="date-slider",
                                    step=86400,  # daily
                                    allowCross=True,
                                ),
                                style={
                                    "whiteSpace": "nowrap",
                                    "paddingLeft": "5%",
                                    "marginTop": "6px",
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
                html.Br(),
                html.Br(),
                dcc.Graph("downloads-overall", style={"visibility": "hidden"}),
                dcc.Graph("downloads-package-version", style={"visibility": "hidden"}),
                dcc.Graph("downloads-python-version", style={"visibility": "hidden"}),
            ],
            style={"transition": "opacity 200ms ease-in", "minHeight": "100vh"},
            is_in=False,
        ),
    ]
