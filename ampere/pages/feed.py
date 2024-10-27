import dash
import pandas as pd
from dash import dash_table, html

from ampere.common import get_db_con
from ampere.styling import AmpereDTStyle

dash.register_page(__name__, name="feed", top_nav=True, order=3)


def create_feed_table() -> pd.DataFrame:
    con = get_db_con()
    return con.sql(
        """
        select
            strftime(event_timestamp, '%Y-%m-%d %H:%M:%S') "event time",
            date_part('day', current_date - event_timestamp)  "days ago",
            concat('[', user_name, ']', '(https://www.github.com/', user_name, ')')  "user",
            repo_name "repo",
            event_type "type",
            event_action "action",
            event_data "description",
            event_id "id"

        from main.mart_feed_events
        order by event_timestamp desc
        """
    ).to_df()


def layout(**kwargs):
    df = create_feed_table()
    return [
        html.Br(),
        dash_table.DataTable(
            df.to_dict("records"),
            columns=[
                (
                    {"id": x, "name": "user name", "presentation": "markdown"}
                    if x == "user"
                    else {"id": x, "name": x}
                )
                for x in df.columns
            ],
            id="tbl",
            **AmpereDTStyle,
        ),
    ]
