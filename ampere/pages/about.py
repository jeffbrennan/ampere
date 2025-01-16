import dash_bootstrap_components as dbc
import pandas as pd
from dash import Input, Output, callback, dash_table, html

from ampere.common import get_frontend_db_con
from ampere.styling import ScreenWidth, get_ampere_dt_style


def create_repo_table() -> pd.DataFrame:
    with get_frontend_db_con() as con:
        df = con.sql(
            """
        select
            concat('[', repo_name, ']', '(https://www.github.com/mrpowers-io/', repo_name, ')') as repo_name,
            forks_count                                                                         as forks,
            stargazers_count                                                                    as stargazers,
            open_issues_count                                                                   as "open issues",
            round(date_part('day', current_date - created_at) / 365, 1)                         as "age (years)",
            strftime(created_at, '%Y-%m-%d')                                                    as created,
            strftime(updated_at, '%Y-%m-%d')                                                    as updated,
        from stg_repos
        order by stargazers_count desc
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
    color = "white" if dark_mode else "black"
    if breakpoint_name in [ScreenWidth.xs, ScreenWidth.sm]:
        font_size = "12px"
        margin_left = "0vw"
    else:
        font_size = "16px"
        margin_left = "12vw"

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
    lg_margins = {"maxWidth": "65vw", "width": "65vw", "marginLeft": "12vw"}

    if breakpoint_name in [ScreenWidth.xs, ScreenWidth.sm]:
        about_style["style_cell"]["font_size"] = "12px"
        about_style["style_table"].update(sm_margins)

    else:
        about_style["style_table"].update(lg_margins)

    # print(about_style)
    tbl = dash_table.DataTable(
        df.to_dict("records"),
        columns=[
            (
                {"id": x, "name": "repo", "presentation": "markdown"}
                if x == "repo_name"
                else {"id": x, "name": x}
            )
            for x in df.columns
        ],
        **about_style,
    )

    return tbl, {}, True


def layout():
    return [
        dbc.Fade(
            id="about-fade",
            children=[
                html.Br(),
                html.Div(id="about-table", style={"visibility": "hidden"}),
                html.Br(),
                html.Div(id="about-text", style={"visibility": "hidden"}),
            ],
            style={"transition": "opacity 200ms ease-in", "minHeight": "100vh"},
            is_in=False,
        )
    ]
