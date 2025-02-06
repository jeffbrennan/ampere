import os
from typing import Any

import dash_bootstrap_components as dbc
import pandas as pd
from dash import Input, Output, callback, dcc, html
from plotly.graph_objects import Figure

from ampere.app_shared import cache, update_tooltip
from ampere.cli.common import CLIEnvironment
from ampere.common import timeit
from ampere.models import get_repos_with_downloads
from ampere.styling import ScreenWidth
from ampere.viz import (
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
    breakpoint_name: str,
) -> tuple[Figure, dict]:
    env = os.environ.get("AMPERE_ENV")

    if date_range == date_bounds and env == "prod":
        mode = "dark" if dark_mode else "light"
        f_name = f"downloads_{repo}_{group}_{mode}_{breakpoint_name}"
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
        screen_width=ScreenWidth(breakpoint_name),
    )

    return fig, {}


@cache.memoize()
@timeit
def dash_get_repos_with_downloads():
    return get_repos_with_downloads(CLIEnvironment.dev)


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
        Input("breakpoints", "widthBreakpoint"),
    ],
)
def viz_downloads_overall(
    df_data: list[dict],
    date_range: list[int],
    dark_mode: bool,
    date_bounds: list[int],
    repo_name: str,
    breakpoint_name: str,
):
    return get_viz_downloads(
        df_data=df_data,
        date_range=date_range,
        dark_mode=dark_mode,
        date_bounds=date_bounds,
        group="overall",
        repo=repo_name,
        breakpoint_name=breakpoint_name,
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
        Input("breakpoints", "widthBreakpoint"),
    ],
)
def viz_downloads_by_package_version(
    df_data: list[dict],
    date_range: list[int],
    dark_mode: bool,
    date_bounds: list[int],
    repo_name: str,
    breakpoint_name: str,
):
    return get_viz_downloads(
        df_data=df_data,
        date_range=date_range,
        dark_mode=dark_mode,
        date_bounds=date_bounds,
        group="package_version",
        repo=repo_name,
        breakpoint_name=breakpoint_name,
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
        Input("breakpoints", "widthBreakpoint"),
    ],
)
def viz_downloads_by_python_version(
    df_data: list[dict],
    date_range: list[int],
    dark_mode: bool,
    date_bounds: list[int],
    repo_name: str,
    breakpoint_name: str,
):
    return get_viz_downloads(
        df_data=df_data,
        date_range=date_range,
        dark_mode=dark_mode,
        date_bounds=date_bounds,
        group="python_version",
        repo=repo_name,
        breakpoint_name=breakpoint_name,
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
    Input("breakpoints", "widthBreakpoint"),
)
@timeit
def toggle_slider_tooltip_visibility(breakpoint_name: str) -> dict[Any, Any]:
    return update_tooltip(breakpoint_name)


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
    df["download_date"] = pd.to_datetime(df["download_date"])
    min_timestamp = df["download_date"].min().timestamp()
    max_timestamp = df["download_date"].max().timestamp()

    date_slider_value = [min_timestamp, max_timestamp]
    date_slider_marks = {
        min_timestamp: {"style": {"fontSize": 0}},
        max_timestamp: {"style": {"fontSize": 0}},
    }
    return (
        min_timestamp,
        max_timestamp,
        date_slider_value,
        date_slider_marks,
        [min_timestamp, max_timestamp],
    )


@callback(
    [
        Output("dl-repo-filter-width", "width"),
        Output("dl-date-filter-width", "width"),
        Output("dl-filter-padding-width", "width"),
        Output("dl-filter-row", "style"),
    ],
    Input("breakpoints", "widthBreakpoint"),
)
def update_filter_for_mobile(breakpoint_name: str):
    filter_style = {"top": "60px"}
    if breakpoint_name == ScreenWidth.xs:
        filter_style.update({"paddingTop": "20px"})
        return 4, 7, 1, filter_style

    if breakpoint_name == ScreenWidth.sm:
        return 3, 5, 4, filter_style

    if breakpoint_name == ScreenWidth.md:
        return 2, 4, 6, filter_style

    return 1, 4, 7, filter_style


@callback(
    Output("repo-selection", "style"),
    Input("breakpoints", "widthBreakpoint"),
)
def update_dropdown_font_size(breakpoint_name: str):
    if breakpoint_name == ScreenWidth.xs:
        return {"fontSize": "12px"}

    if breakpoint_name == ScreenWidth.sm:
        return {"fontSize": "14px"}

    return {"fontSize": "20px"}


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
                            id="dl-repo-filter-width",
                            style={"marginLeft": "5%"},
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
                            id="dl-date-filter-width",
                        ),
                        dbc.Col(id="dl-filter-padding-width"),
                    ],
                    id="dl-filter-row",
                ),
                html.Br(),
                dcc.Graph(
                    "downloads-overall",
                    style={"visibility": "hidden"},
                    config={"displayModeBar": False},
                ),
                dcc.Graph(
                    "downloads-package-version",
                    style={"visibility": "hidden"},
                    config={"displayModeBar": False},
                ),
                dcc.Graph(
                    "downloads-python-version",
                    style={"visibility": "hidden"},
                    config={"displayModeBar": False},
                ),
            ],
            style={"transition": "opacity 200ms ease-in", "minHeight": "100vh"},
            is_in=False,
        ),
    ]
