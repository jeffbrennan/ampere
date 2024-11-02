import copy

import dash
import dash_breakpoints
import pandas as pd
from dash import Input, Output, callback, dash_table, html

from ampere.common import get_db_con
from ampere.styling import AmpereDTStyle, ScreenWidth

dash.register_page(__name__, name="feed", top_nav=True, order=3)


def create_feed_table() -> pd.DataFrame:
    con = get_db_con()
    return con.sql(
        """
        select
            strftime(event_timestamp, '%Y-%m-%d') "date",
            strftime(event_timestamp, '%H:%M') "time",
            concat('[', user_name, ']', '(https://www.github.com/', user_name, ')')  "user",
            event_action "action",
            event_type "type",
            date_part('day', current_date - event_timestamp)  "days ago",
            repo_name "repo",
            coalesce(event_data, '') "description",
            event_link
        from main.mart_feed_events
        order by "date" desc, "time" desc
        """
    ).to_df()


@callback(
    [
        Output("feed-table", "style_table"),
        Output("feed-table", "style_cell_conditional"),
    ],
    [
        Input("feed-table", "style_table"),
        Input("feed-table", "style_cell_conditional"),
        Input("breakpoints", "widthBreakpoint"),
    ],
)
def handle_table_margins(
    style_table_incoming: dict,
    style_cell_conditional_incoming: list[dict],
    breakpoint_name: str,
) -> tuple[dict, list[dict]]:
    style_table = copy.deepcopy(style_table_incoming)
    style_cell_conditional = copy.deepcopy(style_cell_conditional_incoming)
    style_cell_conditional = [
        i for i in style_cell_conditional if "minWidth" not in str(i)
    ]

    if breakpoint_name in ["lg", "xl"]:
        width_lookup = {"date": 80, "time": 45, "event": 400, "description": 400}
        style_table["maxWidth"] = "65vw"
        style_table["width"] = "65vw"
        style_table["marginLeft"] = "12vw"

    elif breakpoint_name == "md":
        width_lookup = {"date": 100, "time": 60, "event": 400, "description": 400}
        style_table = style_feed_table()["style_table"]

    elif breakpoint_name == "sm":
        width_lookup = {"date": 100, "time": 60, "event": 250, "description": 250}
        style_table = style_feed_table()["style_table"]

    elif breakpoint_name == "xs":
        width_lookup = {"date": 100, "time": 60, "event": 200, "description": 200}
        style_table = style_feed_table()["style_table"]
    else:
        raise ValueError(f"unhandled breakpoint name: {breakpoint_name}")

    margin_adjustment = [
        {
            "if": {"column_id": i},
            "minWidth": width_lookup[i],
            "maxWidth": width_lookup[i],
        }
        for i in ["date", "time", "event", "description"]
    ]

    style_cell_conditional.extend(margin_adjustment)
    return style_table, style_cell_conditional


def style_feed_table() -> dict:
    feed_style = copy.deepcopy(AmpereDTStyle)
    feed_style["css"] = [
        dict(selector="p", rule="margin-bottom: 0; padding: 10px; text-align: left;"),
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
    df["event"] = df["event"].str.replace("created a [fork", "[forked")
    df["event"] = df["event"].str.replace("a [issue", "an [issue")
    df["event"] = df["event"].str.replace(" 0 days ago", " today")
    df["event"] = df["event"].str.replace(" 1 days ago", " yesterday")
    df.loc[df["type"] == "fork", "event"] = df["event"].str.replace(" in ", " ")

    df_final = df[["date", "time", "event", "description"]]
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
        dash_breakpoints.WindowBreakpoints(
            id="breakpoints",
            widthBreakpointThresholdsPx=[
                500,
                1200,
                1920,
                2560,
            ],
            widthBreakpointNames=[i.value for i in ScreenWidth],
        ),
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
            id="feed-table",
            **feed_style,
        ),
    ]
