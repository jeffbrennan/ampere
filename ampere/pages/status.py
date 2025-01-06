from copy import deepcopy

import dash
import dash_bootstrap_components as dbc
import pandas as pd
from dash import Input, Output, callback, dash_table, html

from ampere.common import get_frontend_db_con
from ampere.styling import AmpereDTStyle

dash.register_page(__name__, name="status", top_nav=True, order=2)


def create_status_summary_table() -> pd.DataFrame:
    with get_frontend_db_con() as con:
        df = con.sql(
            """
        select
            summary,
            downloads,
            feed,
            issues,
            network_stargazers,
            network_followers
        from 
        mart_status_summary
        """
        ).to_df()
    return df


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
            case when "timestamp" is not null
                then concat(strftime(timestamp, '%Y-%m-%d %H:%M:%S'), ' (UTC)')
            end "timestamp",
            round((extract(epoch from now()) - extract(epoch from "timestamp")) / 3600, 2) as hours_stale,
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


def style_summary_table() -> dict:
    summary_style = deepcopy(AmpereDTStyle)
    summary_style["style_table"].update(
        {"height": "25%", "maxWidth": "65vw", "width": "65vw", "marginLeft": "12vw"}
    )
    summary_style["css"] = [
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

    summary_style["filter_action"] = "none"
    summary_style["sort_action"] = "none"
    summary_style["style_data_conditional"] = [
        {
            "if": {"state": "active"},
            "backgroundColor": "transparent",
            "border": "1px solid black",
        }
    ]

    del summary_style["style_header"]["paddingRight"]
    return summary_style


def style_details_table() -> dict:
    status_details_style = deepcopy(AmpereDTStyle)
    status_details_style["style_table"].update(
        {
            "maxWidth": "65vw",
            "width": "65vw",
            "marginLeft": "12vw",
            "height": "85vh",
            "maxHeight": "85vh",
        }
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
    status_details_style["style_cell_conditional"].append(
        {
            "if": {"column_id": "records"},
            "type": "numeric",
            "format": {"specifier": ",d"},
        }
    )
    return status_details_style


def layout():
    details_df = create_status_table()
    details_cols = []
    for col in details_df.columns:
        if col in ["model"]:
            details_cols.append({"name": col, "id": col, "presentation": "markdown"})
        elif col == "records":
            details_cols.append(
                {"name": col, "id": col, "type": "numeric", "format": {"specifier": ",d"}}
            )
        else:
            details_cols.append({"name": col, "id": col})

    summary_df = create_status_summary_table()

    return [
        dbc.Fade(
            id="status-fade",
            children=[
                html.Br(),
                dash_table.DataTable(
                    summary_df.to_dict("records"),
                    columns=[{"id": x, "name": x} for x in summary_df.columns],
                    id="status-summary-table",
                    **style_summary_table(),
                ),
                html.Br(),
                html.Br(),
                html.Br(),
                dash_table.DataTable(
                    details_df.to_dict("records"),
                    columns=details_cols,
                    id="status-table",
                    **style_details_table(),
                ),
            ],
            style={"transition": "opacity 300ms ease-in"},
            is_in=False,
        )
    ]
