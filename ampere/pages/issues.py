import copy

import dash_bootstrap_components as dbc
import pandas as pd
from dash import Input, Output, callback, dash_table, html

from ampere.common import get_frontend_db_con, timeit
from ampere.styling import (
    ColumnInfo,
    ScreenWidth,
    get_ampere_dt_style,
    get_table_title_style,
    style_dt_background_colors_by_rank,
)


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

    xs_col_widths = {
        "repo": 100,
        "author": 100,
        "date": 105,
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
        "xs": xs_col_widths,
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


def handle_summary_col_widths(
    style_cell_conditional: list[dict], breakpoint_name: str
) -> list[dict]:
    style_cell_conditional = [
        i for i in style_cell_conditional if "minWidth" not in str(i)
    ]
    if breakpoint_name in [ScreenWidth.xl, ScreenWidth.lg]:
        col_width = 50
    elif breakpoint_name in [ScreenWidth.md, ScreenWidth.sm]:
        col_width = 60
    else:
        col_width = 90

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

    if breakpoint_name in [ScreenWidth.xs, ScreenWidth.sm]:
        style["fontSize"] = "16px"

    return style


@callback(
    [
        Output("summary-title", "children"),
        Output("summary-title", "style"),
        Output("issues-fade", "is_in"),
    ],
    [
        Input("summary-table", "children"),
        Input("breakpoints", "widthBreakpoint"),
        Input("color-mode-switch", "value"),
    ],
)
def display_summary_title(_, breakpoint_name, dark_mode: bool):
    table_title_style = get_table_title_style(dark_mode)
    summary_title = html.Label(
        "summary", style=handle_title_margins(table_title_style, breakpoint_name)
    )

    return summary_title, {}, True


@callback(
    [
        Output("summary-table", "children"),
        Output("summary-table", "style"),
    ],
    [
        Input("color-mode-switch", "value"),
        Input("breakpoints", "widthBreakpoint"),
    ],
)
@timeit
def get_styled_issues_summary_table(dark_mode: bool, breakpoint_name: str):
    summary_df = create_issues_summary_table()
    summary_data = summary_df.to_dict("records")
    n_repos = summary_df.shape[0]

    summary_style = get_ampere_dt_style(dark_mode)
    del summary_style["style_table"]["maxHeight"]
    del summary_style["style_table"]["height"]

    formatting_cols = [
        ColumnInfo(name="open issues", ascending=True, palette="oranges"),
        ColumnInfo(name="median issue age (days)", ascending=True, palette="oranges"),
        ColumnInfo(name="new issues (this month)", ascending=True, palette="oranges"),
        ColumnInfo(name="closed issues (this month)", ascending=True, palette="greens"),
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
    summary_style["style_data_conditional"].extend(col_value_heatmaps)

    summary_style["style_cell_conditional"] = handle_summary_col_widths(
        summary_style["style_cell_conditional"], breakpoint_name
    )

    summary_style["style_table"] = handle_table_margins(
        summary_style["style_table"], breakpoint_name
    )

    if breakpoint_name in [ScreenWidth.xs, ScreenWidth.sm]:
        summary_style["style_table"]["font_size"] = "12px"

    tbl = (
        dash_table.DataTable(
            data=summary_data,
            columns=[
                {"id": x, "name": x, "presentation": "markdown"}
                if x in ["repo"]
                else {"id": x, "name": x}
                for x in summary_df.columns
            ],
            **summary_style,
        ),
    )
    return tbl, {}


@callback(
    [
        Output("issues-title", "children"),
        Output("issues-title", "style"),
    ],
    [
        Input("issues-table", "children"),
        Input("breakpoints", "widthBreakpoint"),
        Input("color-mode-switch", "value"),
    ],
)
def display_issues_title(_, breakpoint_name, dark_mode: bool):
    table_title_style = get_table_title_style(dark_mode)
    issues_title = html.Label(
        "issues",
        style=handle_title_margins(table_title_style, breakpoint_name),
    )

    return issues_title, {}


@callback(
    [
        Output("issues-table", "children"),
        Output("issues-table", "style"),
    ],
    [
        Input("color-mode-switch", "value"),
        Input("breakpoints", "widthBreakpoint"),
    ],
)
@timeit
def get_styled_issues_table(dark_mode: bool, breakpoint_name: str):
    issues_df = create_issues_table()
    base_style = get_ampere_dt_style(dark_mode)
    base_style["style_cell_conditional"] = handle_col_widths(
        base_style["style_cell_conditional"], breakpoint_name
    )

    base_style["style_table"] = handle_table_margins(
        base_style["style_table"], breakpoint_name
    )

    if breakpoint_name in [ScreenWidth.xs, ScreenWidth.sm]:
        base_style["style_table"]["font_size"] = "12px"

    tbl = (
        dash_table.DataTable(
            issues_df.to_dict("records"),
            columns=[
                {"id": x, "name": x, "presentation": "markdown"}
                if x in ["repo", "author", "title", "body"]
                else {"id": x, "name": x}
                for x in issues_df.columns
            ],
            **base_style,
        ),
    )

    return tbl, {}


def layout():
    return dbc.Fade(
        id="issues-fade",
        children=[
            html.Div(id="summary-title", style={"visibility": "hidden"}),
            html.Div(id="summary-table", style={"visibility": "hidden"}),
            html.Br(),
            html.Div(id="issues-title", style={"visibility": "hidden"}),
            html.Div(id="issues-table", style={"visibility": "hidden"}),
        ],
        style={"transition": "opacity 200ms ease-in", "minHeight": "100vh"},
        is_in=False,
    )
