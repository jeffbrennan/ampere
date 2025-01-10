import dash
import dash_bootstrap_components as dbc
import pandas as pd
from dash import Input, Output, callback, dash_table, html

from ampere.common import get_frontend_db_con
from ampere.styling import AmperePalette, get_ampere_dt_style

dash.register_page(__name__, name="feed", top_nav=True, order=3)


def create_feed_table() -> pd.DataFrame:
    with get_frontend_db_con() as con:
        df = con.sql(
            """    
        select
            strftime(event_timestamp, '%Y-%m-%d')                                   as "date",
            strftime(event_timestamp, '%H:%M')                                      as "time",
            concat('[', user_name, ']', '(https://www.github.com/', user_name, ')') as "user",
            event_action                                                            as "action",
            event_type                                                              as "type",
            date_part('day', current_date - event_timestamp)                        as "days ago",
            repo_name                                                               as "repo",
            coalesce(event_data, '')                                                as "description",
            event_link
        from main.mart_feed_events
        order by "date" desc, "time" desc
        """,
        ).to_df()
    return df


def handle_table_margins(breakpoint_name: str):
    style_table = {}
    if breakpoint_name in ["lg", "xl"]:
        width_lookup = {"date": 80, "time": 45, "event": 400, "description": 400}
        style_table = {"maxWidth": "65vw", "width": "65vw", "marginLeft": "12vw"}

    elif breakpoint_name == "md":
        width_lookup = {"date": 100, "time": 60, "event": 400, "description": 400}

    elif breakpoint_name == "sm":
        width_lookup = {"date": 100, "time": 60, "event": 250, "description": 250}

    elif breakpoint_name == "xs":
        width_lookup = {"date": 100, "time": 60, "event": 200, "description": 200}
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

    return style_table, margin_adjustment


@callback(
    [
        Output("feed-table", "style_table"),
        Output("feed-table", "style_cell_conditional"),
        Output("feed-table", "style_data_conditional"),
        Output("feed-table", "style_filter"),
        Output("feed-table", "style_header"),
        Output("feed-table", "css"),
        Output("feed-fade", "is_in"),
    ],
    [
        Input("breakpoints", "widthBreakpoint"),
        Input("color-mode-switch", "value"),
    ],
)
def style_feed_table(breakpoint_name: str, dark_mode: bool = False):
    default_style = get_ampere_dt_style(dark_mode)
    style_table = default_style["style_table"]
    style_cell_conditional = default_style["style_cell_conditional"]
    style_data_conditional = default_style["style_data_conditional"]

    if dark_mode:
        text_color = "white"
        event_background_colors = {
            "pull request": "#263302",
            "issue": "#6b303a",
            "star": "#54401b",
            "commit": "#15524e",
            "fork": "#3b2133",
        }
        event_color_border = AmperePalette.TABLE_EVEN_ROW_COLOR_DARK
    else:
        text_color = "black"
        event_background_colors = {
            "pull request": "#d9e6b5",
            "issue": "#edb4bd",
            "star": "#e8d3a9",
            "commit": "#b6dedc",
            "fork": "#e1ccdb",
        }
        event_color_border = AmperePalette.TABLE_EVEN_ROW_COLOR_LIGHT

    color_styles = [
        {
            "if": {"filter_query": f"{{event}} contains '{k}'", "column_id": "event"},
            "backgroundColor": v,
            "borderBottom": f"1px solid {event_color_border}",
        }
        for k, v in event_background_colors.items()
    ]

    standard_col_colors = [
        {
            "color": text_color,
            "borderLeft": f"2px solid {text_color}",
            "borderRight": f"2px solid {text_color}",
        }
        for _ in ["date", "time", "event", "description"]
    ]

    style_data_conditional.extend(color_styles + standard_col_colors)

    adjusted_style_table, adjusted_style_cell_conditional = handle_table_margins(
        breakpoint_name
    )

    style_table.update(adjusted_style_table)
    style_cell_conditional.extend(adjusted_style_cell_conditional)

    return (
        style_table,
        style_cell_conditional,
        style_data_conditional,
        default_style["style_filter"],
        default_style["style_header"],
        default_style["css"],
        True,
    )


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


def layout():
    raw_df = create_feed_table()
    df = format_feed_table(raw_df)

    return [
        dbc.Fade(
            id="feed-fade",
            children=[
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
                    id="feed-table",
                    **get_ampere_dt_style(),
                ),
            ],
            style={"transition": "opacity 200ms ease-in"},
            is_in=False,
        )
    ]
