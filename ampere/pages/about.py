import datetime

import dash
import pandas as pd
from dash import dash_table, html

from ampere.common import get_db_con
from ampere.styling import AmpereDTStyle

dash.register_page(__name__, name="about", top_nav=True, order=2)


def get_last_updated() -> datetime.datetime:
    con = get_db_con()
    max_retrieved_at = (
        con.sql("SELECT max(retrieved_at) as last_updated FROM main.repos")
        .to_df()
        .squeeze()
    )

    return max_retrieved_at


def create_repo_table() -> pd.DataFrame:
    con = get_db_con()
    return con.sql(
        """SELECT
            concat('[', repo_name, ']', '(https://www.github.com/mrpowers-io/', repo_name, ')')   repo_name,
            forks_count  forks,
            stargazers_count  stargazers,
            open_issues_count  "open issues",
            round(date_part('day', current_date - created_at)  / 365, 1)  "age (years)",
            created_at created,
            updated_at updated
        FROM main.repos
        ORDER BY stargazers_count DESC
        """
    ).to_df()


def layout(**kwargs):
    df = create_repo_table()
    last_updated = get_last_updated()
    last_updated_str = last_updated.strftime("%Y-%m-%d")
    return [
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
            **AmpereDTStyle,
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
    ]
