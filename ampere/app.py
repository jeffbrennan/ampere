import dash
import dash_bootstrap_components as dbc
import dash_breakpoints
from dash import Input, Output, callback, dcc, html

from ampere.styling import AmperePalette, ScreenWidth

app = dash.Dash(
    use_pages=True,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    suppress_callback_exceptions=True,
)
server = app.server


@callback(
    [
        Output("downloads-link", "style"),
        Output("feed-link", "style"),
        Output("issues-link", "style"),
        Output("network-link", "toggle_style"),
        Output("about-link", "style"),
    ],
    Input("current-url", "pathname"),
)
def update_downloads_link_color(pathname: str):
    output_styles = [
        {"color": AmperePalette.BRAND_TEXT_COLOR_MUTED},
        {"color": AmperePalette.BRAND_TEXT_COLOR_MUTED},
        {"color": AmperePalette.BRAND_TEXT_COLOR_MUTED},
        {"color": AmperePalette.BRAND_TEXT_COLOR_MUTED},
        {"color": AmperePalette.BRAND_TEXT_COLOR_MUTED},
    ]

    if pathname in ["/", "network"]:
        return output_styles

    pages = ["downloads", "feed", "issues", "network", "about"]
    current_page = pathname.removeprefix("/").split("-")[0]

    output_styles[pages.index(current_page)] = {
        "color": AmperePalette.PAGE_ACCENT_COLOR,
        "backgroundColor": "white",
        "borderRadius": "10px",
    }

    return output_styles


navbar = dbc.NavbarSimple(
    children=[
        dcc.Location(id="current-url", refresh=False),
        dbc.NavItem(
            dbc.NavLink(
                id="downloads-link",
                children="downloads",
                href="downloads",
                style={"color": AmperePalette.BRAND_TEXT_COLOR_MUTED},
                class_name="downloads-link",
            )
        ),
        dbc.NavItem(
            dbc.NavLink(
                id="feed-link",
                children="feed",
                href="feed",
                style={"color": AmperePalette.BRAND_TEXT_COLOR_MUTED},
            )
        ),
        dbc.NavItem(
            dbc.NavLink(
                id="issues-link",
                children="issues",
                href="issues",
                style={"color": AmperePalette.BRAND_TEXT_COLOR_MUTED},
            )
        ),
        dbc.DropdownMenu(
            children=[
                dbc.DropdownMenuItem(
                    "stargazers",
                    href="network-stargazers",
                    style={"color": AmperePalette.PAGE_ACCENT_COLOR},
                ),
                dbc.DropdownMenuItem(
                    "followers",
                    href="network-followers",
                    style={"color": AmperePalette.PAGE_ACCENT_COLOR},
                ),
            ],
            id="network-link",
            nav=True,
            in_navbar=True,
            label="networks",
            toggle_style={"color": AmperePalette.BRAND_TEXT_COLOR_MUTED},
        ),
        dbc.NavItem(
            dbc.NavLink(
                id="about-link",
                children="about",
                href="about",
                style={"color": AmperePalette.BRAND_TEXT_COLOR_MUTED},
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
    app.run(host="0.0.0.0")
