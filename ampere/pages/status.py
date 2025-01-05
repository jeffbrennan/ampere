from copy import deepcopy

import dash
import dash_bootstrap_components as dbc
import pandas as pd
from dash import Input, Output, callback, dash_table, html

from ampere.common import get_frontend_db_con
from ampere.styling import AmpereDTStyle

dash.register_page(__name__, name="status", top_nav=True, order=2)


def create_status_table() -> pd.DataFrame:
    with get_frontend_db_con() as con:
        df = con.sql(
            """
        select
            model,
            page,
            timestamp_col,
            timestamp,
            records
            from mart_status_details
        """,
        ).to_df()

    return df


@callback(
    Output("status-fade", "is_in"),
    Input("status-table", "id"),
)
def status_table_fadein(_: str) -> bool:
    return True


def layout():
    df = create_status_table()
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
            id="status-fade",
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
                    id="status-table",
                    **about_style,
                ),
                html.Hr(),

            ],
            style={"transition": "opacity 300ms ease-in"},
            is_in=False,
        )
    ]
