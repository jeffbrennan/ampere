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
            concat(
                '[', model, ']',
                '(https://github.com/jeffbrennan/ampere/tree/main/models/',
                 model_folder, '/', "model", '.sql)'
            ) as "model",
            array_to_string(page, ', ') as pages,
            timestamp_col,
            timestamp,
            round((extract(epoch FROM now()) - extract(epoch FROM "timestamp")) / 3600, 2) as hours_stale,
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
    status_details_style = deepcopy(AmpereDTStyle)
    status_details_style["style_table"].update(
        {"height": "75%", "maxWidth": "65vw", "width": "65vw", "marginLeft": "12vw"}
    )
    status_details_style["style_cell"]["textAlign"] = "left"
    status_details_style["css"] = [
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
        dbc.Fade(
            id="status-fade",
            children=[
                html.Br(),
                html.Hr(),
                html.Br(),
                dash_table.DataTable(
                    df.to_dict("records"),
                    columns=[
                        (
                            {"id": x, "name": x, "presentation": "markdown"}
                            if x in ["model"]
                            else {"id": x, "name": x}
                        )
                        for x in df.columns
                    ],
                    id="status-table",
                    **status_details_style,
                ),
            ],
            style={"transition": "opacity 300ms ease-in"},
            is_in=False,
        )
    ]
