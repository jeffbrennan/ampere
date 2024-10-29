import dash
import pandas as pd
from dash import dash_table, html

from ampere.common import get_db_con
from ampere.styling import AmpereDTStyle
from copy import deepcopy

dash.register_page(__name__, name="feed", top_nav=True, order=3)


def create_feed_table() -> pd.DataFrame:
    con = get_db_con()
    return con.sql(
        """
        select
            strftime(event_timestamp, '%Y-%m-%d %H:%M:%S') "event time",
            event_type "type",
            event_action "action",
            repo_name "repo",
            concat('[', user_name, ']', '(https://www.github.com/', user_name, ')')  "user",
            coalesce(event_data, '') "description",
            date_part('day', current_date - event_timestamp)  "days ago",
            concat('[', replace(event_link, 'https://github.com/', ''), ']', '(', event_link, ')') "link"

        from main.mart_feed_events
        order by event_timestamp desc
        """
    ).to_df()


def style_feed_table() -> dict:
    feed_style = deepcopy(AmpereDTStyle)
    feed_style["css"] = [
        dict(selector="p", rule="margin-bottom: 0; text-align: center;"),
    ]

    colors = {
        "pull request": "#d9e6b5",
        "issue": "#edb4bd",
        "star": "#e8d3a9",
        "commit": "#b6dedc",
        "fork": "#e1ccdb",
    }

    color_styles = [
        {
            "if": {"filter_query": f"{{type}} = '{k}'", "column_id": "type"},
            "backgroundColor": v,
            "color": "black",
            "borderBottom": "1px rgb(237, 237, 237) solid",
        }
        for k, v in colors.items()
    ]

    feed_style["style_data_conditional"].extend(color_styles)

    return feed_style


def layout(**kwargs):
    df = create_feed_table()
    feed_style = style_feed_table()
    return [
        html.Br(),
        html.Br(),
        dash_table.DataTable(
            df.to_dict("records"),
            columns=[
                (
                    {"id": x, "name": x, "presentation": "markdown"}
                    if x in ["user", "link", "description"]
                    else {"id": x, "name": x}
                )
                for x in df.columns
            ],
            id="tbl",
            **feed_style,
        ),
    ]
