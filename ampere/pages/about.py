from copy import deepcopy

import dash
import dash_bootstrap_components as dbc
import pandas as pd
from dash import Input, Output, callback, dash_table, html

from ampere.common import get_frontend_db_con
from ampere.styling import AmpereDTStyle

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


@callback(
    Output("about-fade", "is_in"),
    Input("about-table", "id"),  # dummy input for callback trigger
)
def about_table_fadein(_: str) -> bool:
    return True


def layout():
    df = create_repo_table()
    about_style = deepcopy(AmpereDTStyle)
    about_style["style_table"]["height"] = "50%"
    about_style["css"] = [
        dict(
            selector="p",
            rule="""
                   margin-bottom: 0;
                   padding-bottom: 15px;
                   padding-top: 15px;
                   padding-left: 5px;
                   padding-right: 5px;
                   text-align: center;
               """,
        ),
    ]

    return [
        dbc.Fade(
            id="about-fade",
            children=[
                html.Br(),
                dash_table.DataTable(
                    df.to_dict("records"),
                    columns=[
                        (
                            {"id": x, "name": "repo", "presentation": "markdown"}
                            if x == "repo_name"
                            else {"id": x, "name": x}
                        )
                        for x in df.columns
                    ],
                    id="about-table",
                    **about_style,
                ),
                html.Hr(),
                html.Div(
                    [
                        html.Div("made with ❤️ by ", style={"display": "inline"}),
                        html.A(
                            "Jeff Brennan",
                            href="https://github.com/jeffbrennan",
                            target="_blank",
                        ),
                        html.Div(" and ", style={"display": "inline"}),
                        html.A(
                            "mrpowers-io",
                            href="https://github.com/mrpowers-io",
                            target="_blank",
                        ),
                        html.Div(" contributors", style={"display": "inline"}),
                    ]
                ),
            ],
            style={"transition": "opacity 300ms ease-in"},
            is_in=False,
        )
    ]
