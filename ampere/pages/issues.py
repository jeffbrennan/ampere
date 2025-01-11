import copy

import dash
import dash_bootstrap_components as dbc
import pandas as pd
from dash import Input, Output, callback, dash_table, dcc, html

from ampere.common import get_frontend_db_con
from ampere.styling import (
    AmperePalette,
    ColumnInfo,
    get_ampere_dt_style,
    style_dt_background_colors_by_rank,
    table_title_style,
)

dash.register_page(__name__, name="feed", top_nav=True, order=3)


def create_issues_table() -> pd.DataFrame:
    with get_frontend_db_con() as con:
        df = con.sql(
            """
       select
           repo,
           author,
           title,
           body,
           "date",
           "days old",
           "comments"
       from mart_issues
       order by repo, "days old"
        """,
        ).to_df()
    return df


def create_issues_summary_table() -> pd.DataFrame:
    with get_frontend_db_con() as con:
        df = con.sql(
            """
        select repo,
            "open issues",
            "median issue age (days)",
            "new issues (this month)",
            "closed issues (this month)"
        from mart_issues_summary
        order by "open issues" desc
        """
        ).to_df()
    return df


@callback(
    Output("issues-table", "style_cell_conditional"),
    [
        Input("issues-table", "style_cell_conditional"),
        Input("breakpoints", "widthBreakpoint"),
    ],
)
def handle_col_widths(
    style_cell_conditional_incoming: list[dict], breakpoint_name: str
) -> list[dict]:
    style_cell_conditional = copy.deepcopy(style_cell_conditional_incoming)
    style_cell_conditional = [
        i for i in style_cell_conditional if "minWidth" not in str(i)
    ]

    col_widths = {
        "repo": 60,
        "author": 90,
        "date": 50,
        "days old": 35,
        "comments": 45,
        "title": 150,
        "body": 300,
    }

    small_col_widths = {
        "repo": 100,
        "author": 180,
        "date": 100,
        "days old": 80,
        "comments": 100,
        "title": 250,
        "body": 400,
    }
    width_lookup = {
        "xl": col_widths,
        "lg": col_widths,
        "md": small_col_widths,
        "sm": small_col_widths,
        "xs": small_col_widths,
    }

    width_adjustment = [
        {
            "if": {"column_id": i},
            "minWidth": width_lookup[breakpoint_name][i],
            "maxWidth": width_lookup[breakpoint_name][i],
        }
        for i in [
            "repo",
            "author",
            "date",
            "days old",
            "comments",
            "title",
            "body",
        ]
    ]

    style_cell_conditional.extend(width_adjustment)
    return style_cell_conditional


@callback(
    Output("summary-table", "style_cell_conditional"),
    [
        Input("summary-table", "style_cell_conditional"),
        Input("breakpoints", "widthBreakpoint"),
    ],
)
def handle_summary_col_widths(
    style_cell_conditional_incoming: list[dict], breakpoint_name: str
) -> list[dict]:
    style_cell_conditional = copy.deepcopy(style_cell_conditional_incoming)
    style_cell_conditional = [
        i for i in style_cell_conditional if "minWidth" not in str(i)
    ]
    if breakpoint_name in ["lg", "xl"]:
        col_width = 50
    else:
        col_width = 60

    style_cell_conditional.append(
        {"if": {"column_id": "repo"}, "minWidth": col_width, "maxWidth": col_width}
    )
    return style_cell_conditional


def handle_table_margins(style_table_incoming: dict, breakpoint_name: str) -> dict:
    style_table = copy.deepcopy(style_table_incoming)
    if breakpoint_name != "xl":
        style_table.pop("maxWidth", None)
        style_table.pop("width", None)
        style_table.pop("marginLeft", None)
        return style_table

    margin_adjustments = {"maxWidth": "65vw", "width": "65vw", "marginLeft": "12vw"}
    style_table.update(margin_adjustments)
    return style_table


def handle_title_margins(style_incoming: dict, breakpoint_name: str) -> dict:
    margin_adjustments = {
        "xl": {"maxWidth": "65vw", "width": "65vw", "marginLeft": "12vw"},
        "other": {"maxWidth": "90vw", "width": "90vw", "marginLeft": "0vw"},
    }

    style = copy.deepcopy(style_incoming)
    if breakpoint_name == "xl":
        margin_adjustment = margin_adjustments["xl"]
    else:
        margin_adjustment = margin_adjustments["other"]

    style.update(margin_adjustment)
    return style


@callback(
    Output("summary-title", "style"),
    [
        Input("summary-title", "style"),
        Input("breakpoints", "widthBreakpoint"),
    ],
)
def summary_title_margin_callback(*args):
    return handle_title_margins(*args)


@callback(
    Output("summary-table", "style_table"),
    [
        Input("summary-table", "style_table"),
        Input("breakpoints", "widthBreakpoint"),
        Input("color-mode-switch", "value"),
    ],
)
def summary_table_margin_callback(*args):
    updated_style = handle_table_margins(*args[:2])
    dark_mode = args[2]
    color = "white" if dark_mode else "black"
    updated_style.update(
        {
            "borderTop": f"2px {color} solid",
            "borderLeft": f"2px {color} solid",
            "borderBottom": f"2px {color} solid",
        }
    )
    return updated_style


@callback(
    Output("issues-title", "style"),
    [
        Input("issues-title", "style"),
        Input("breakpoints", "widthBreakpoint"),
    ],
)
def issues_title_margin_callback(*args, **kwargs):
    return handle_title_margins(*args, **kwargs)


@callback(
    Output("issues-table", "style_table"),
    [
        Input("issues-table", "style_table"),
        Input("breakpoints", "widthBreakpoint"),
    ],
)
def issues_table_margin_callback(*args, **kwargs):
    return handle_table_margins(*args, **kwargs)


@callback(
    [
        Output("summary-table", "style_data_conditional"),
        Output("summary-table", "style_filter"),
        Output("summary-table", "style_header"),
        Output("summary-table", "css"),
    ],
    [
        Input("summary-data", "data"),
        Input("color-mode-switch", "value"),
    ],
)
def style_issues_summary_table(summary_data: list[dict], dark_mode: bool):
    summary_style = get_ampere_dt_style(dark_mode)
    summary_df = pd.DataFrame(summary_data)
    n_repos = summary_df.shape[0]

    formatting_cols = [
        ColumnInfo(name="open issues", ascending=True, palette="oranges"),
        ColumnInfo(name="median issue age (days)", ascending=True, palette="oranges"),
        ColumnInfo(name="new issues (this month)", ascending=True, palette="oranges"),
        ColumnInfo(name="closed issues (this month)", ascending=True, palette="greens"),
    ]
    if dark_mode:
        text_color = "white"
    else:
        text_color = "black"

    standard_col_colors = [
        {
            "color": text_color,
            "borderLeft": "none",
            "borderRight": f"2px solid {text_color}",
        }
        for _ in summary_df.columns
    ]
    col_value_heatmaps = style_dt_background_colors_by_rank(
        df=summary_df,
        n_bins=n_repos,
        cols=formatting_cols,
        dark_mode=dark_mode,
    )

    summary_style["style_data_conditional"] = [
        i for i in summary_style["style_data_conditional"] if "odd" not in str(i)
    ]

    if dark_mode:
        border_color = "white"
        odd_row_color = AmperePalette.PAGE_BACKGROUND_COLOR_DARK
    else:
        border_color = "black"
        odd_row_color = AmperePalette.PAGE_BACKGROUND_COLOR_LIGHT

    summary_style["style_data_conditional"].append(
        {
            "if": {"row_index": "odd"},
            "backgroundColor": odd_row_color,
            "borderBottom": f"1px {border_color} solid",
            "borderTop": f"1px {border_color} solid",
        }
    )
    summary_style["style_data_conditional"].extend(
        standard_col_colors + col_value_heatmaps
    )

    return (
        summary_style["style_data_conditional"],
        summary_style["style_filter"],
        summary_style["style_header"],
        summary_style["css"],
    )


@callback(
    [
        Output("issues-table", "style_data_conditional"),
        Output("issues-table", "style_filter"),
        Output("issues-table", "style_header"),
        Output("issues-table", "css"),
        Output("issues-fade", "is_in"),
    ],
    [
        Input("issues-table", "data"),
        Input("color-mode-switch", "value"),
    ],
)
def style_issues_table(issues_data: list[dict], dark_mode: bool):
    issues_df = pd.DataFrame(issues_data)
    base_style = get_ampere_dt_style(dark_mode)
    if dark_mode:
        text_color = "white"
    else:
        text_color = "black"

    standard_col_colors = [
        {
            "color": text_color,
            "borderLeft": f"2px solid {text_color}",
            "borderRight": f"2px solid {text_color}",
        }
        for _ in issues_df.columns
    ]
    base_style["style_data_conditional"].extend(standard_col_colors)
    return (
        base_style["style_data_conditional"],
        base_style["style_filter"],
        base_style["style_header"],
        base_style["css"],
        True,
    )


def layout():
    df = create_issues_table()
    summary_df = create_issues_summary_table()
    summary_data = summary_df.to_dict("records")

    summary_base_style = get_ampere_dt_style()
    del summary_base_style["style_table"]["maxHeight"]
    del summary_base_style["style_table"]["height"]

    return [
        dcc.Store(id="summary-data", data=summary_data),
        dbc.Fade(
            id="issues-fade",
            children=[
                html.Br(),
                html.Label("summary", style=table_title_style, id="summary-title"),
                dash_table.DataTable(
                    data=summary_data,
                    columns=[
                        {"id": x, "name": x, "presentation": "markdown"}
                        if x in ["repo"]
                        else {"id": x, "name": x}
                        for x in summary_df.columns
                    ],
                    id="summary-table",
                    **summary_base_style,
                ),
                html.Br(),
                html.Label("issues", style=table_title_style, id="issues-title"),
                dash_table.DataTable(
                    df.to_dict("records"),
                    columns=[
                        {"id": x, "name": x, "presentation": "markdown"}
                        if x in ["repo", "author", "title", "body"]
                        else {"id": x, "name": x}
                        for x in df.columns
                    ],
                    id="issues-table",
                    **get_ampere_dt_style(),
                ),
            ],
            style={"transition": "opacity 300ms ease-in"},
            is_in=False,
        ),
    ]
