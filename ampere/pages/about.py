import datetime
from copy import deepcopy

import dash
import pandas as pd
from dash import dash_table, html, dcc

from ampere.common import get_db_con
from ampere.styling import AmpereDTStyle

dash.register_page(__name__, name="about", top_nav=True, order=2)


def get_last_updated() -> datetime.datetime:
    con = get_db_con()
    max_retrieved_at = (
        con.sql("select max(retrieved_at) as last_updated from main.repos")
        .to_df()
        .squeeze()
    )

    return max_retrieved_at


def create_repo_table() -> pd.DataFrame:
    con = get_db_con()
    return con.sql(
        """
        select
            concat('[', repo_name, ']', '(https://www.github.com/mrpowers-io/', repo_name, ')') as repo_name,
            forks_count                                                                         as forks,
            stargazers_count                                                                    as stargazers,
            open_issues_count                                                                   as "open issues",
            round(date_part('day', current_date - created_at) / 365, 1)                         as "age (years)",
            strftime(created_at, '%Y-%m-%d')                                                    as created,
            strftime(updated_at, '%Y-%m-%d')                                                    as updated,
        from main.repos
        order by stargazers_count desc
        """
    ).to_df()


def layout():
    df = create_repo_table()
    last_updated = get_last_updated()
    last_updated_str = last_updated.strftime("%Y-%m-%d")
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
        dcc.Loading(
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
                    id="tbl",
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
                html.P(f"last updated: {last_updated_str}"),
            ],
            delay_hide=400,
            delay_show=0,
            fullscreen=True,
        )
    ]
