import datetime
from typing import Any, Optional

import dash
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.express as px
import pytz
from dash import Input, Output, callback, dcc, html
from plotly.graph_objs import Figure

from ampere.app_shared import cache
from ampere.common import get_frontend_db_con
from ampere.styling import AmperePalette, ScreenWidth
from ampere.viz import generate_repo_palette

dash.register_page(__name__, name="summary", path="/", top_nav=True, order=0)


def viz_summary(
    df: pd.DataFrame,
    metric_type: str,
    date_range: Optional[list[int]] = None,
    show_fig: bool = False,
    screen_width: ScreenWidth = ScreenWidth.lg,
    dark_mode: bool = False,
) -> Figure:
    df_filtered = df.query(f"metric_type == '{metric_type}'").sort_values("metric_date")
    if date_range is not None:
        filter_date_min = datetime.datetime.fromtimestamp(
            date_range[0], tz=pytz.timezone("America/New_York")
        )
        filter_date_max = datetime.datetime.fromtimestamp(
            date_range[1], tz=pytz.timezone("America/New_York")
        )

        df_filtered = df_filtered.query(f"metric_date >= '{filter_date_min}'").query(
            f"metric_date <= '{filter_date_max}'"
        )

    if dark_mode:
        font_color = "white"
        bg_color = AmperePalette.PAGE_BACKGROUND_COLOR_DARK
        template = "plotly_dark"
    else:
        font_color = "black"
        bg_color = AmperePalette.PAGE_BACKGROUND_COLOR_LIGHT
        template = "plotly_white"

    repo_palette = generate_repo_palette()
    fig = px.area(
        df_filtered,
        x="metric_date",
        y="metric_count",
        color="repo_name",
        template=template,
        hover_name="repo_name",
        color_discrete_map=repo_palette,
        height=500,
        category_orders={"repo_name": repo_palette.keys()},
        facet_col="metric_type",  # single var facet col for plot title
    )
    fig.update_layout(plot_bgcolor=bg_color, paper_bgcolor=bg_color)
    fig.for_each_annotation(
        lambda a: a.update(
            text="<b>" + a.text.split("=")[-1] + "</b>",
            font_size=18,
            bgcolor=AmperePalette.PAGE_ACCENT_COLOR2,
            font_color="white",
            borderpad=5,
        )
    )
    fig.update_yaxes(matches=None, showticklabels=True, showgrid=False)
    fig.update_xaxes(showgrid=False)
    fig.update_traces(hovertemplate="<b>%{x}</b><br>n=%{y}")

    fig_legend_y = {ScreenWidth.xs: 1.04, ScreenWidth.sm: 1.02}
    if screen_width in [ScreenWidth.xs, ScreenWidth.sm]:
        fig.update_layout(
            legend=dict(
                title=None,
                itemsizing="constant",
                font=dict(size=14),
                orientation="h",
                yanchor="top",
                y=fig_legend_y[screen_width],
                xanchor="center",
                x=0.5,
            )
        )
    else:
        fig.update_layout(
            legend=dict(title=None, itemsizing="constant", font=dict(size=14))
        )

    fig.for_each_annotation(
        lambda a: a.update(
            text="<b>" + a.text.split("=")[-1] + "</b>",
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
            linecolor=font_color,
            mirror=True,
            tickfont_size=14,
        )
    )
    fig.for_each_xaxis(
        lambda x: x.update(
            title="",
            showline=True,
            linewidth=1,
            linecolor=font_color,
            mirror=True,
            showticklabels=True,
            tickfont_size=14,
        )
    )

    fig.update_layout(margin=dict(l=0, r=0))
    if show_fig:
        fig.show()

    return fig


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
        Output("summary-stars", "style"),
    ],
    [
        Input("summary-df", "data"),
        Input("breakpoints", "widthBreakpoint"),
        Input("summary-date-slider", "value"),
        Input("color-mode-switch", "value"),
    ],
)
def viz_summary_stars(
    df_data: list[dict], breakpoint_name: str, date_range: list[int], dark_mode: bool
):
    fig = viz_summary(
        df=pd.DataFrame(df_data),
        metric_type="stars",
        date_range=date_range,
        screen_width=ScreenWidth(breakpoint_name),
        dark_mode=dark_mode,
    )

    return fig, {}


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
    ],
)
def viz_summary_issues(
    df_data: list[dict], breakpoint_name: str, date_range: list[int], dark_mode: bool
):
    fig = viz_summary(
        df=pd.DataFrame(df_data),
        metric_type="issues",
        date_range=date_range,
        screen_width=ScreenWidth(breakpoint_name),
        dark_mode=dark_mode,
    )
    return fig, {}


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
    ],
)
def viz_summary_commits(
    df_data: list[dict], breakpoint_name: str, date_range: list[int], dark_mode: bool
):
    fig = viz_summary(
        df=pd.DataFrame(df_data),
        metric_type="commits",
        date_range=date_range,
        screen_width=ScreenWidth(breakpoint_name),
        dark_mode=dark_mode,
    )

    return fig, {}


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
                dcc.Graph("summary-stars", style={"visibility": "hidden"}),
                dcc.Graph("summary-issues", style={"visibility": "hidden"}),

                dcc.Graph("summary-commits", style={"visibility": "hidden"}),
            ],
            style={"transition": "opacity 200ms ease-in", "minHeight": "100vh"},
            is_in=False,
        ),
    ]
