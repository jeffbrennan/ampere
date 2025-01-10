import argparse
import time

import dash
import dash_bootstrap_components as dbc
import dash_breakpoints
from dash import Input, Output, callback, dcc, html

from ampere.app_shared import cache
from ampere.styling import AmperePalette, ScreenWidth


@callback(
    [
        Output("downloads-link", "style"),
        Output("feed-link", "style"),
        Output("issues-link", "style"),
        Output("network-link", "toggle_style"),
        Output("status-link", "style"),
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
        {"color": AmperePalette.BRAND_TEXT_COLOR_MUTED},
    ]

    if pathname in ["/", "network"]:
        return output_styles

    pages = ["downloads", "feed", "issues", "network", "status", "about"]
    current_page = pathname.removeprefix("/").split("-")[0]

    output_styles[pages.index(current_page)] = {
        "color": AmperePalette.PAGE_ACCENT_COLOR,
        "backgroundColor": "white",
        "borderRadius": "10px",
    }

    return output_styles


@callback(
    Output("ampere-page", "style"),
    Input("color-mode-switch", "value"),
)
def update_page_color(dark_mode: bool):
    base_style = {"paddingLeft": "5%", "paddingRight": "5%", "paddingBottom": "3%"}
    time.sleep(0.15)
    if dark_mode:
        base_style["backgroundColor"] = AmperePalette.PAGE_BACKGROUND_COLOR_DARK
    else:
        base_style["backgroundColor"] = AmperePalette.PAGE_BACKGROUND_COLOR_LIGHT

    return base_style


def layout():
    navbar = dbc.Navbar(
        dbc.Container(
            [
                dbc.NavbarBrand("ampere", href="/", style={"fontWeight": "bold"}),
                dbc.NavbarToggler(id="navbar-toggler"),
                dbc.Collapse(
                    dbc.Nav(
                        [
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
                                toggle_style={
                                    "color": AmperePalette.BRAND_TEXT_COLOR_MUTED
                                },
                            ),
                            dbc.NavItem(
                                dbc.NavLink(
                                    id="status-link",
                                    children="status",
                                    href="status",
                                    style={"color": AmperePalette.BRAND_TEXT_COLOR_MUTED},
                                )
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
                        className="ml-auto",
                        navbar=True,
                    ),
                    id="navbar-collapse",
                    navbar=True,
                ),
                dbc.NavItem(
                    html.Span(
                        [
                            dbc.Switch(
                                id="color-mode-switch",
                                value=False,
                                className="d-inline-block ms-1",
                                persistence=True,
                            ),
                            dbc.Label("🌛", html_for="color-mode-switch"),
                        ]
                    ),
                ),
            ],
            fluid=True,
        ),
        color=AmperePalette.PAGE_ACCENT_COLOR,
        dark=True,
        fixed="top",
        style={"width": "100%"},
    )

    return dbc.Container(
        id="ampere-page",
        children=[
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
    envs = {
        "prod": {"host": "0.0.0.0", "debug": False, "serve_locally": False},
        "dev": {"host": "127.0.0.1", "debug": True, "serve_locally": True},
    }
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--env",
        type=str,
        default="prod",
        help="Environment to run the app in. Default is prod",
    )

    env = parser.parse_args().env

    app = dash.Dash(
        use_pages=True,
        external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.FONT_AWESOME],
        suppress_callback_exceptions=True,
        compress=True,
        update_title="",
        serve_locally=envs[env]["serve_locally"],
    )
    server = app.server
    cache.init_app(server)

    app.layout = layout()
    app.run(host=envs[env]["host"], debug=envs[env]["debug"])
