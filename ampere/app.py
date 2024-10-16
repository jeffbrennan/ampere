import datetime

import dash
import dash_bootstrap_components as dbc
from dash import html, callback, Input, Output

from ampere.common import get_db_con

app = dash.Dash(
    __name__,
    use_pages=True,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
)


def get_last_updated() -> datetime.datetime:
    con = get_db_con()
    max_retrieved_at = (
        con.sql("SELECT max(retrieved_at) as last_updated FROM main.repos")
        .to_df()
        .squeeze()
    )

    return max_retrieved_at


last_updated = get_last_updated()
last_updated_str = last_updated.strftime("%Y-%m-%d")

navbar = dbc.NavbarSimple(
    children=[
        dbc.DropdownMenu(
            children=[
                dbc.DropdownMenuItem("stargazers", href="network-stargazers"),
                dbc.DropdownMenuItem("followers", href="network-followers"),
            ],
            nav=True,
            in_navbar=True,
            label="networks",
        ),
        dbc.NavItem(dbc.NavLink("about", href="about")),
    ],
    brand="ampere",
    brand_href="/",
    color="#3F6DF9",
    className="mb-2",
    dark=True,
    fluid=True,
    links_left=True,
    style={
        "width": "100%",
        "color": "#FFFFFF",
    },
)

footer = html.Footer(
    children=[
        html.Div(
            f"last updated {last_updated_str}",
            style={"textAlign": "left", "fontSize": "14px"},
        )
    ],
    style={
        "position": "fixed",
        "bottom": "0",
        "width": "100%",
        "backgroundColor": "#3F6DF9",
        "color": "#FFFFFF",
        "paddingLeft": "1%",
        "left": "0",
    },
)
app.layout = dbc.Container(
    [navbar, dash.page_container, footer],
    fluid=True,
    style={
        "paddingLeft": "5%",
        "paddingRight": "5%",
    },
)

if __name__ == "__main__":
    app.run_server(debug=True)
