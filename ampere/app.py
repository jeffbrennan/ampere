import dash
import dash_bootstrap_components as dbc
import dash_breakpoints
from dash import html

from ampere.styling import AmperePalette, ScreenWidth

app = dash.Dash(
    use_pages=True,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    suppress_callback_exceptions=True,
)

navbar = dbc.NavbarSimple(
    children=[
        dbc.DropdownMenu(
            children=[
                dbc.DropdownMenuItem(
                    "stargazers",
                    href="network-stargazers",
                    style={"color": "black"},
                ),
                dbc.DropdownMenuItem(
                    "followers", href="network-followers", style={"color": "black"}
                ),
            ],
            nav=True,
            in_navbar=True,
            label="networks",
            toggle_style={"color": AmperePalette.BRAND_TEXT_COLOR_MUTED},
        ),
        dbc.NavItem(
            dbc.NavLink(
                "feed",
                href="feed",
                style={"color": AmperePalette.BRAND_TEXT_COLOR_MUTED},
                class_name="navbar-text",
            )
        ),
        dbc.NavItem(
            dbc.NavLink(
                "issues",
                href="issues",
                style={"color": AmperePalette.BRAND_TEXT_COLOR_MUTED},
                class_name="navbar-text",
            )
        ),
        dbc.NavItem(
            dbc.NavLink(
                "about",
                href="about",
                style={"color": AmperePalette.BRAND_TEXT_COLOR_MUTED},
                class_name="navbar-text",
            )
        ),
    ],
    color=AmperePalette.PAGE_ACCENT_COLOR,
    dark=True,
    fixed="top",
    fluid=True,
    style={"width": "100%"},
    brand="ampere",
    brand_style={"fontWeight": "bold"},
    links_left=True,
    brand_href="/",
)

app.layout = dbc.Container(
    [
        navbar,
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
        dash.page_container,
    ],
    fluid=True,
    style={"paddingLeft": "5%", "paddingRight": "5%", "paddingBottom": "3%"},
)

if __name__ == "__main__":
    app.run_server(debug=True)
