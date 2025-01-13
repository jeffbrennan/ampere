import dash
import dash_bootstrap_components as dbc
import pandas as pd
from dash import Input, Output, callback, dash_table, html

from ampere.common import get_frontend_db_con, timeit
from ampere.styling import get_ampere_dt_style

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


def create_status_details_table() -> pd.DataFrame:
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


@callback(
    [
        Output("status-summary-table", "children"),
        Output("status-summary-table", "style"),
    ],
    Input("color-mode-switch", "value"),
)
@timeit
def get_styled_summary_table(dark_mode: bool):
    summary_df = create_status_summary_table()
    summary_style = get_ampere_dt_style(dark_mode)
    summary_style["style_data_conditional"] = []

    summary_style["style_table"].update(
        {"height": "25%", "maxWidth": "65vw", "width": "65vw", "marginLeft": "12vw"}
    )
    summary_style["filter_action"] = "none"
    summary_style["sort_action"] = "none"
    summary_style["style_data_conditional"] = [
        {
            "if": {"state": "active"},
            "backgroundColor": "transparent",
            "border": f"1px solid {'white' if dark_mode else 'black'}",
        }
    ]

    del summary_style["style_header"]["paddingRight"]

    tbl = dash_table.DataTable(
        data=summary_df.to_dict("records"),
        columns=[{"id": x, "name": x} for x in summary_df.columns],
        **summary_style,
    )

    print("updating summary table")
    return tbl, {}


@callback(
    [
        Output("status-details-table", "children"),
        Output("status-details-table", "style"),
    ],
    Input("color-mode-switch", "value"),
)
def get_styled_details_table(dark_mode: bool):
    details_df = create_status_details_table()
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

    status_details_style = get_ampere_dt_style(dark_mode)
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
    tbl = (
        dash_table.DataTable(
            details_df.to_dict("records"),
            columns=details_cols,
            id="status-table",
            **status_details_style,
        ),
    )

    return tbl, {}


def layout():
    return [
        dbc.Fade(
            id="status-fade",
            children=[
                html.Br(),
                html.Div(id="status-summary-table", style={"visibility": "hidden"}),
                html.Br(),
                html.Br(),
                html.Div(id="status-details-table", style={"visibility": "hidden"}),
            ],
            style={"transition": "opacity 200ms ease-in", "minHeight": "100vh"},
            is_in=False,
        ),
    ]
