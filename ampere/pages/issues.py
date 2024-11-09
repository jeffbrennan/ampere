import copy

import dash
import pandas as pd
from dash import Input, Output, callback, dash_table, html, dcc

from ampere.common import get_db_con
from ampere.styling import (
    AmpereDTStyle,
    ColumnInfo,
    style_dt_background_colors_by_rank,
    table_title_style,
)

dash.register_page(__name__, name="feed", top_nav=True, order=3)


def create_issues_table() -> pd.DataFrame:
    con = get_db_con()
    return con.sql(
        """        
        select
            concat('[', repo_name, ']', '(https://www.github.com/mrpowers-io/', repo_name, ')') as "repo",
            concat('[', user_name, ']', '(https://www.github.com/', user_name, ')')             as "author",
            concat('[', a.issue_title, ']',
                   '(https://github.com/mrpowers-io/', c.repo_name,
                   '/issues/', a.issue_number, ')'
            )                                                                                   as "title",
            coalesce(a.issue_body, '')                                                          as "body",
            strftime(a.created_at, '%Y-%m-%d')                                                  as "date",
            date_part('day', current_date - a.created_at)                                       as "days old",
            a.comments_count                                                                    as "comments"
        from issues a
        join users b
             on a.author_id = b.user_id
        join repos c
             on a.repo_id = c.repo_id
        where a.state = 'open'
        order by repo_name, a.created_at
        """
    ).to_df()


def create_issues_summary_table() -> pd.DataFrame:
    con = get_db_con()
    return con.sql(
        """with
    closed_issues_past_month as (
        select
            repo_id,
            count(issue_id) as closed_issues
        from issues
        where
            state = 'closed'
            and closed_at >= current_date - 120
        group by repo_id
    ),
    open_issues_base as (
        select
            repo_id,
            issue_id,
            date_part('day', current_date - created_at) as age_days
        from issues
        where
            state = 'open'
    ),
    open_issues as (
        select
            repo_id,
            count(issue_id) as open_issues_count,
            avg(age_days)   as avg_age_days
        from open_issues_base
        group by repo_id
    ),
    new_issues as (
        select repo_id,
        count(issue_id) as new_issues_count,
        from issues
        where state = 'open'
        and created_at >= current_date - 120
        group by repo_id
    ),
    repo_spine as (
        select
            repo_id,
            repo_name
        from repos
    )
select
    concat('[', a.repo_name, ']', '(https://www.github.com/mrpowers-io/', a.repo_name, ')') as "repo",
    coalesce(b.open_issues_count, 0) as "open issues",
    ceil(coalesce(b.avg_age_days, 0))     as "avg issue age (days)",
    coalesce(d.new_issues_count, 0) as "new issues (this month)",
    coalesce(c.closed_issues, 0)     as "closed issues (this month)",
from repo_spine a
left join open_issues b
    on a.repo_id = b.repo_id
left join closed_issues_past_month c
    on a.repo_id = c.repo_id
left join new_issues d
on a.repo_id = d.repo_id
    order by open_issues_count desc
        """
    ).to_df()


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
    [Input("summary-title", "style"), Input("breakpoints", "widthBreakpoint")],
)
def summary_title_margin_callback(*args, **kwargs):
    return handle_title_margins(*args, **kwargs)


@callback(
    Output("summary-table", "style_table"),
    [Input("summary-table", "style_table"), Input("breakpoints", "widthBreakpoint")],
)
def summary_table_margin_callback(*args, **kwargs):
    return handle_table_margins(*args, **kwargs)


@callback(
    Output("issues-title", "style"),
    [Input("issues-title", "style"), Input("breakpoints", "widthBreakpoint")],
)
def issues_title_margin_callback(*args, **kwargs):
    return handle_title_margins(*args, **kwargs)


@callback(
    Output("issues-table", "style_table"),
    [Input("issues-table", "style_table"), Input("breakpoints", "widthBreakpoint")],
)
def issues_table_margin_callback(*args, **kwargs):
    return handle_table_margins(*args, **kwargs)


def style_issues_summary_table(summary_df: pd.DataFrame) -> dict:
    summary_style = copy.deepcopy(AmpereDTStyle)
    del summary_style["style_table"]["maxHeight"]
    del summary_style["style_table"]["height"]
    n_repos = summary_df.shape[0]

    formatting_cols = [
        ColumnInfo(name="open issues", ascending=True, palette="Oranges"),
        ColumnInfo(name="avg issue age (days)", ascending=True, palette="Oranges"),
        ColumnInfo(name="new issues (this month)", ascending=True, palette="Oranges"),
        ColumnInfo(name="closed issues (this month)", ascending=True, palette="Greens"),
    ]

    summary_style["style_data_conditional"] = style_dt_background_colors_by_rank(
        df=summary_df, n_bins=n_repos, cols=formatting_cols
    )
    return summary_style


def layout():
    cell_padding = [
        dict(
            selector="p",
            rule="""
                    margin-bottom: 0;
                    padding-bottom: 15px;
                    padding-top: 15px;
                    padding-left: 5px;
                    padding-right: 5px;
                    text-align: left;
                """,
        ),
    ]

    df = create_issues_table()

    issues_style = copy.deepcopy(AmpereDTStyle)
    issues_style["css"] = cell_padding
    summary_df = create_issues_summary_table()

    summary_style = style_issues_summary_table(summary_df)
    summary_style["css"] = cell_padding
    return [
        dcc.Loading(
            children=[
                html.Br(),
                html.Label("summary", style=table_title_style, id="summary-title"),
                dash_table.DataTable(
                    summary_df.to_dict("records"),
                    columns=[
                        {"id": x, "name": x, "presentation": "markdown"}
                        if x in ["repo"]
                        else {"id": x, "name": x}
                        for x in summary_df.columns
                    ],
                    id="summary-table",
                    **summary_style,
                ),
            ],
            delay_hide=100,
            delay_show=0,
            fullscreen=True,
        ),
        dcc.Loading(
            children=[
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
                    **issues_style,
                ),
            ],
            delay_hide=100,
            delay_show=0,
            fullscreen=True,
        ),
    ]
