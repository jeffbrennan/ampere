import dash
import pandas as pd
from dash import html, dash_table

from ampere.common import get_db_con

dash.register_page(__name__, name="about", top_nav=True, order=2)


def create_repo_table() -> pd.DataFrame:
    con = get_db_con()
    return con.sql(
        """SELECT
            concat('[', repo_name, ']', '(https://www.github.com/mrpowers-io/', repo_name, ')')   repo_name,
            forks_count  forks,
            stargazers_count  stargazers,
            open_issues_count  open_issues,
            round(date_part('day', current_date - created_at)  / 365, 1)  age_years,
            created_at,
            updated_at
        FROM main.repos
        ORDER BY stargazers_count DESC
        """
    ).to_df()


def layout(**kwargs):
    df = create_repo_table()
    return [
        html.H2("Repos"),
        dash_table.DataTable(
            df.to_dict("records"),
            columns=[
                {"id": x, "name": "", "presentation": "markdown"}
                if x == "repo_name"
                else {"id": x, "name": x}
                for x in df.columns
            ],
            id="tbl",
            sort_action="native",
            sort_mode="multi",
            column_selectable="single",
            row_selectable=False,
            row_deletable=False,
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
    ]
