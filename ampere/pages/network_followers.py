import dash
import dash_breakpoints
from dash import dcc, callback, Output, Input, dash_table
import pandas as pd
from plotly.graph_objects import Figure

from ampere.common import get_db_con
from ampere.viz import viz_follower_network

dash.register_page(__name__)


def create_followers_table() -> pd.DataFrame:
    con = get_db_con()
    return con.sql(
        """
        SELECT
            concat('[', user_name, ']', '(https://www.github.com/', user_name, ')')  user_name,
            full_name as name,
            followers_count as followers,
            following_count as following,
            internal_followers_count as "org followers",
            internal_following_count as "org following",
            internal_followers_pct as "org followers %",
            internal_following_pct as "org following %"
        FROM int_network_follower_details 
        order by followers_count desc
        """
    ).to_df()


def layout(**kwargs):
    df = create_followers_table()
    return [
        dcc.Interval(
            id="network-follower-load-interval",
            n_intervals=0,
            max_intervals=0,
            interval=1,
        ),
        dash_breakpoints.WindowBreakpoints(id="network-follower-breakpoints"),
        dcc.Loading(
            dcc.Graph(
                id="network-follower-graph",
                style={
                    "height": "95vh",
                    "marginLeft": "0vw",
                    "marginRight": "0vw",
                    "width": "100%",
                },
                responsive=True,
            ),
        ),
        dash_table.DataTable(
            df.to_dict("records"),
            columns=[
                {"id": x, "name": "", "presentation": "markdown"}
                if x == "user_name"
                else {"id": x, "name": x}
                for x in df.columns
            ],
            id="tbl",
            sort_action="native",
            sort_mode="multi",
            column_selectable="single",
            row_selectable=False,
            row_deletable=False,
            fixed_rows={"headers": True},
            filter_action="native",
            page_size=100,
            style_header={
                "backgroundColor": "#3F6DF9",
                "padding": "10px",
                "color": "#FFFFFF",
            },
            style_cell={
                "textAlign": "center",
                "minWidth": 95,
                "maxWidth": 95,
                "width": 95,
                "font_size": "14px",
                "whiteSpace": "normal",
                "height": "auto",
            },
            style_table={
                "height": "500px",
                "overflowY": "scroll",
                "overflowX": "scroll",
                "margin": {"b": 100},
            },
        ),
    ]


@callback(
    Output("network-follower-graph", "figure"),
    Input("network-follower-load-interval", "n_intervals"),
)
def show_summary_graph(_: int) -> Figure:
    return viz_follower_network()
