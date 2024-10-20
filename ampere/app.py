import datetime
from enum import StrEnum

import dash
import dash_bootstrap_components as dbc
from dash import html, Input, Output, State

from ampere.common import get_db_con

app = dash.Dash(
    use_pages=True,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
)


class Palette(StrEnum):
    PAGE_ACCENT_COLOR = "#3F6DF9"
    BRAND_TEXT_COLOR = "#FFFFFF"
    BRAND_TEXT_COLOR_MUTED = "#CEE5F2"


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
page_links_collapsed = dbc.Row(
    [
        dbc.Col(
            dbc.DropdownMenu(
                children=[
                    dbc.DropdownMenuItem("stargazers", href="network-stargazers"),
                    dbc.DropdownMenuItem("followers", href="network-followers"),
                ],
                label="networks",
                toggle_style={
                    "color": Palette.BRAND_TEXT_COLOR_MUTED,
                    "backgroundColor": Palette.PAGE_ACCENT_COLOR,
                    "borderColor": Palette.PAGE_ACCENT_COLOR,
                },
                class_name="navbar-text",
            ),
        ),
        dbc.Col(
            dbc.NavItem(
                dbc.NavLink(
                    "about",
                    href="about",
                    style={"color": Palette.BRAND_TEXT_COLOR_MUTED},
                    class_name="navbar-text",
                )
            ),
        ),
    ],
    align="left",
    class_name="row align-items-center",
)

navbar = dbc.Navbar(
    dbc.Container(
        [
            html.A(
                dbc.Row([dbc.NavbarBrand("ampere")]),
                href="/",
                style={"textDecoration": "none"},
            ),
            dbc.NavbarToggler(id="navbar-toggler", n_clicks=0),
            dbc.Collapse(
                page_links_collapsed,
                id="navbar-collapse",
                is_open=False,
                navbar=True,
            ),
            html.P(
                f"last updated {last_updated_str}",
                style={
                    "marginBottom": "0",
                    "color": Palette.BRAND_TEXT_COLOR_MUTED,
                    "fontSize": "12px"
                },
            ),
        ],
        fluid=True,
    ),
    color=Palette.PAGE_ACCENT_COLOR,
    dark=True,
    sticky="top",
    style={"padding": "0"}
)


# add callback for toggling the collapse on small screens
@app.callback(
    Output("navbar-collapse", "is_open"),
    [Input("navbar-toggler", "n_clicks")],
    [State("navbar-collapse", "is_open")],
)
def toggle_navbar_collapse(n, is_open):
    if n:
        return not is_open
    return is_open


app.layout = dbc.Container(
    [navbar, dash.page_container],
    fluid=True,
    style={
        "paddingLeft": "5%",
        "paddingRight": "5%",
    },
)

if __name__ == "__main__":
    app.run_server(debug=True)
