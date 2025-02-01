from typing import Any

import dash_bootstrap_components as dbc
import pandas as pd
from dash import Input, Output, callback, dcc, html
from plotly.graph_objects import Figure

from ampere.app_shared import cache, update_tooltip
from ampere.common import timeit
from ampere.styling import ScreenWidth
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
    breakpoint_name: str,
) -> tuple[Figure, dict]:
    if date_range == date_bounds:
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
    [
        Input("date-slider", "min"),
        Input("date-slider", "max"),
        Input("date-slider", "value"),
        Input("breakpoints", "widthBreakpoint"),
        Input("color-mode-switch", "value"),
    ],
)
@timeit
def toggle_slider_tooltip_visibility(
    min_date_seconds: int,
    max_date_seconds: int,
    date_range: list[int],
    breakpoint_name: str,
    dark_mode: bool,
) -> dict[Any, Any]:
    return update_tooltip(
        min_date_seconds,
        max_date_seconds,
        date_range,
        breakpoint_name,
        dark_mode,
    )


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
    if breakpoint_name in [ScreenWidth.xs, ScreenWidth.sm]:
        filter_style.update({"paddingTop": "20px"})
        return 4, 7, 1, filter_style

    return 2, 4, 7, filter_style


@callback(
    [
        Output("repo-selection", "className"),
        Output("repo-selection", "style"),
    ],
    [
        Input("color-mode-switch", "value"),
        Input("breakpoints", "widthBreakpoint"),
    ],
)
def update_dropdown_menu_color(dark_mode: bool, breakpoint_name: str):
    class_name = "dark-mode" if dark_mode else "light-mode"
    if breakpoint_name in [ScreenWidth.xs, ScreenWidth.sm]:
        font_size = "12px"
    else:
        font_size = "20px"

    style = {
        "borderRadius": "10px",
        "fontSize": font_size,
        "marginRight": "10%",
        "marginTop": "2%",
        "paddingBottom": "2px",
        "paddingTop": "2px",
    }
    return class_name, style


@callback(
    Output("dl-filter-row", "className"),
    Input("color-mode-switch", "value"),
)
def update_filter_colors(dark_mode: bool):
    return "dark-mode" if dark_mode else "light-mode"


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
