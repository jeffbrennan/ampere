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
            concat('[', replace(event_link, 'https://github.com/', ''), ']', '(', event_link, ')') "link"

        from main.mart_feed_events
        order by event_timestamp desc
        """
    ).to_df()


def layout(**kwargs):
    df = create_feed_table()
    feed_style = AmpereDTStyle
    feed_style["css"] = [
        dict(selector="p", rule="margin-bottom: 0; text-align: center;")
    ]
    return [
        html.Br(),
        dash_table.DataTable(
            df.to_dict("records"),
            columns=[
                (
                    {"id": x, "name": x, "presentation": "markdown"}
                    if x in ["user", "link"]
                    else {"id": x, "name": x}
                )
                for x in df.columns
            ],
            id="tbl",
            **feed_style,
        ),
    ]
