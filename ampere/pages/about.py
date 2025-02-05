import dash_bootstrap_components as dbc
import pandas as pd
from dash import Input, Output, callback, dash_table, html

from ampere.common import get_frontend_db_con, timeit
from ampere.styling import (
    ScreenWidth,
    get_ampere_colors,
    get_ampere_dt_style,
)


@timeit
def create_repo_table() -> pd.DataFrame:
    with get_frontend_db_con() as con:
        df = con.sql(
            """
        with downloads_total as (
            select
                repo,
                sum(download_count) as total_downloads
            from mart_downloads_summary
            where group_name = 'overall'
            group by all
        ),
        downloads_past_week as (
            select
                repo,
                download_count as downloads_past_week
            from mart_downloads_summary
            where group_name = 'overall'
            and download_date = (select max(download_date) from mart_downloads_summary) 
       )
        select
            concat('[', a.repo_name, ']', '(https://www.github.com/mrpowers-io/', a.repo_name, ')') as repo_name,
            a.forks_count                                                                           as forks,
            a.stargazers_count                                                                      as stargazers,
            a.open_issues_count                                                                     as "open issues",
            round(date_part('day', current_date - a.created_at) / 365, 1)                           as "age (years)",
            strftime(a.created_at, '%Y-%m-%d')                                                      as created,
            strftime(a.updated_at, '%Y-%m-%d')                                                      as updated,
            b.total_downloads                                                                       as "downloads (total)",
            c.downloads_past_week                                                                   as "downloads (past week)"
        from stg_repos a
        left join downloads_total as b on a.repo_name = b.repo
        left join downloads_past_week as c on a.repo_name = c.repo
        order by a.stargazers_count desc
        """,
        ).to_df()

    return df


@callback(
    [
        Output("about-text", "children"),
        Output("about-text", "style"),
    ],
    [
        Input("color-mode-switch", "value"),
        Input("breakpoints", "widthBreakpoint"),
    ],
)
def get_styled_about_text(dark_mode: bool, breakpoint_name: str):
    _, color = get_ampere_colors(dark_mode, False)

    if breakpoint_name in [ScreenWidth.xs, ScreenWidth.sm]:
        font_size = "12px"
        margin_left = "0vw"
    else:
        font_size = "14px"
        margin_left = "20vw"

    return [
        html.Div(
            id="about-text",
            children=[
                html.Div(
                    "made with ❤️ by ",
                    style={"display": "inline", "color": color, "fontSize": font_size},
                ),
                html.A(
                    "Jeff Brennan",
                    href="https://github.com/jeffbrennan",
                    target="_blank",
                    style={"fontSize": font_size},
                ),
                html.Div(
                    " and ",
                    style={"display": "inline", "color": color, "fontSize": font_size},
                ),
                html.A(
                    "mrpowers-io",
                    href="https://github.com/mrpowers-io",
                    target="_blank",
                    style={"fontSize": font_size},
                ),
                html.Div(
                    " contributors",
                    style={"display": "inline", "color": color, "fontSize": font_size},
                ),
            ],
            style={"marginLeft": margin_left},
        ),
    ], {}


@callback(
    [
        Output("about-table", "children"),
        Output("about-table", "style"),
        Output("about-fade", "is_in"),
    ],
    [
        Input("color-mode-switch", "value"),
        Input("breakpoints", "widthBreakpoint"),
    ],
)
def get_styled_about_table(dark_mode: bool, breakpoint_name: str):
    df = create_repo_table()
    about_style = get_ampere_dt_style(dark_mode)
    about_style["style_table"]["height"] = "auto"

    sm_margins = {"maxWidth": "90vw", "width": "90vw"}
    lg_margins = {"maxWidth": "50vw", "width": "50vw", "marginLeft": "20vw"}

    if breakpoint_name in [ScreenWidth.xs, ScreenWidth.sm]:
        about_style["style_cell"]["font_size"] = "12px"
        about_style["style_table"].update(sm_margins)

    else:
        about_style["style_table"].update(lg_margins)

    tbl_cols = []
    for col in df.columns:
        if col == "repo_name":
            tbl_cols.append(
                {"id": "repo_name", "name": "repo", "presentation": "markdown"}
            )
        elif "downloads" in col:
            tbl_cols.append(
                {"name": col, "id": col, "type": "numeric", "format": {"specifier": ",d"}}
            )
        else:
            tbl_cols.append({"id": col, "name": col})

    tbl = dash_table.DataTable(
        df.to_dict("records"),
        columns=tbl_cols,
        **about_style,
    )

    return tbl, {}, True


def layout():
    return [
        dbc.Fade(
            id="about-fade",
            children=[
                html.Div(id="about-table", style={"visibility": "hidden"}),
                html.Br(),
                html.Div(id="about-text", style={"visibility": "hidden"}),
            ],
            style={"transition": "opacity 200ms ease-in", "minHeight": "100vh"},
            is_in=False,
        )
    ]
