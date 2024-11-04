import copy

import dash
import pandas as pd
from dash import Input, Output, callback, dash_table, html

from ampere.common import get_db_con
from ampere.styling import AmpereDTStyle

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


@callback(
    [
        Output("issues-table", "style_table"),
        Output("issues-table", "style_cell_conditional"),
    ],
    [
        Input("issues-table", "style_table"),
        Input("issues-table", "style_cell_conditional"),
        Input("breakpoints", "widthBreakpoint"),
    ],
)
def handle_table_margins(
    style_table_incoming: dict,
    style_cell_conditional_incoming: list[dict],
    breakpoint_name: str,
) -> tuple[dict, list[dict]]:
    style_table = copy.deepcopy(style_table_incoming)
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

    if breakpoint_name == "xl":
        style_table["maxWidth"] = "65vw"
        style_table["width"] = "65vw"
        style_table["marginLeft"] = "12vw"
    else:
        style_table = AmpereDTStyle["style_table"]

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
    return style_table, style_cell_conditional


def layout():
    df = create_issues_table()
    issues_style = copy.deepcopy(AmpereDTStyle)
    issues_style["css"] = [
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

    return [
        html.Br(),
        html.Br(),
        dash_table.DataTable(
            df.to_dict("records"),
            columns=[
                (
                    {"id": x, "name": x, "presentation": "markdown"}
                    if x in ["repo", "author", "title", "body"]
                    else {"id": x, "name": x}
                )
                for x in df.columns
            ],
            id="issues-table",
            **issues_style,
        ),
    ]
