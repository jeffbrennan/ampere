from copy import deepcopy

import dash
import dash_bootstrap_components as dbc
import pandas as pd
from dash import Input, Output, callback, dash_table, html

from ampere.common import get_frontend_db_con
from ampere.styling import AmperePalette, get_ampere_dt_style

dash.register_page(__name__, name="about", top_nav=True, order=2)


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


def get_styled_about_text(dark_mode: bool):
    color = "white" if dark_mode else "black"
    return [
        html.Div("made with ❤️ by ", style={"display": "inline", "color": color}),
        html.A(
            "Jeff Brennan",
            href="https://github.com/jeffbrennan",
            target="_blank",
        ),
        html.Div(" and ", style={"display": "inline", "color": color}),
        html.A(
            "mrpowers-io",
            href="https://github.com/mrpowers-io",
            target="_blank",
        ),
        html.Div(" contributors", style={"display": "inline", "color": color}),
    ]


@callback(
    [
        Output("about-contents", "children"),
        Output("about-contents", "is_in"),
    ],
    Input("color-mode-switch", "value"),
)
def get_styled_about_table(dark_mode: bool):
    df = create_repo_table()
    about_style = get_ampere_dt_style(dark_mode)
    about_style["style_table"]["maxHeight"] = "50%"
    about_style["style_table"]["height"] = "50%"

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
    about_text = get_styled_about_text(dark_mode)
    children = [html.Br(), tbl] + about_text

    parent_container = html.Div(
        children=children,
        style={
            "backgroundColor": AmperePalette.PAGE_BACKGROUND_COLOR_LIGHT
            if not dark_mode
            else AmperePalette.PAGE_BACKGROUND_COLOR_DARK,
            "minHeight": "100vh",
        },
    )

    return parent_container, True


@callback(
    [
        Output("about-text", "children"),
        Output("about-text", "style"),
    ],
    Input("color-mode-switch", "value"),
)
def layout():
    return [
        dbc.Fade(
            id="about-contents",
            style={"transition": "opacity 200ms ease-in"},
            is_in=False,
        )
    ]
