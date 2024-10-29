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
            concat('[', user_name, ']', '(https://www.github.com/', user_name, ')')  "user",
            event_action "action",
            event_type "type",
            date_part('day', current_date - event_timestamp)  "days ago",
            repo_name "repo",
            coalesce(event_data, '') "description",
            event_link
        from main.mart_feed_events
        order by "event time" desc
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
            "if": {"filter_query": f"{{event}} contains '{k}'", "column_id": "event"},
            "backgroundColor": v,
            "color": "black",
            "borderBottom": "1px rgb(237, 237, 237) solid",
        }
        for k, v in colors.items()
    ]

    feed_style["style_data_conditional"].extend(color_styles)

    return feed_style


def format_feed_table(df: pd.DataFrame) -> pd.DataFrame:
    df["type_link"] = "[" + df["type"] + "]" + "(" + df["event_link"] + ")"
    df["repo_link"] = (
        "[" + df["repo"] + "]" + "(https://github.com/mrpowers-io/" + df["repo"] + ")"
    )
    df["type_link"] = df["type_link"].fillna("star")

    df["event"] = (
        df["user"]
        + " "
        + df["action"]
        + " a "
        + df["type_link"]
        + " in "
        + df["repo_link"]
        + " "
        + df["days ago"].astype(str)
        + " days ago"
    )
    df["event"] = df["event"].str.replace("created a star in", "starred")
    df["event"] = df["event"].str.replace("a [issue", "an [issue")
    df["event"] = df["event"].str.replace(" 0 days ago", " today")
    df["event"] = df["event"].str.replace(" 1 days ago", " yesterday")

    print(df["event"][28])
    df_final = df[["event", "event time", "description"]]
    if not isinstance(df_final, pd.DataFrame):
        raise TypeError()

    return df_final


def layout(**kwargs):
    raw_df = create_feed_table()
    df = format_feed_table(raw_df)

    feed_style = style_feed_table()
    return [
        html.Br(),
        html.Br(),
        dash_table.DataTable(
            df.to_dict("records"),
            columns=[
                (
                    {"id": x, "name": x, "presentation": "markdown"}
                    if x in ["event", "description"]
                    else {"id": x, "name": x}
                )
                for x in df.columns
            ],
            id="tbl",
            **feed_style,
        ),
    ]
