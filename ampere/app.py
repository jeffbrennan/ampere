import argparse

import dash
import dash_bootstrap_components as dbc
import dash_breakpoints
from dash import Input, Output, State, callback, dcc, html

from ampere.app_shared import cache
from ampere.pages import (
    about,
    downloads,
    feed,
    issues,
    network_followers,
    network_stargazers,
    status,
    summary,
)
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
    [
        Input("current-url", "pathname"),
        Input("color-mode-switch", "value"),
    ],
)
def update_downloads_link_color(pathname: str, dark_mode: bool):
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
    if dark_mode:
        text_color = AmperePalette.BRAND_TEXT_COLOR_MUTED
        background_color = AmperePalette.PAGE_BACKGROUND_COLOR_DARK
    else:
        text_color = AmperePalette.PAGE_ACCENT_COLOR
        background_color = AmperePalette.PAGE_BACKGROUND_COLOR_LIGHT

    output_styles[pages.index(current_page)] = {
        "color": text_color,
        "backgroundColor": background_color,
        "borderRadius": "10px",
    }

    return output_styles


@callback(
    Output("ampere-page", "style"),
    Input("color-mode-switch", "value"),
)
def update_page_color(dark_mode: bool):
    base_style = {"paddingLeft": "5%", "paddingRight": "5%", "paddingBottom": "3%"}
    if dark_mode:
        base_style["backgroundColor"] = AmperePalette.PAGE_BACKGROUND_COLOR_DARK
    else:
        base_style["backgroundColor"] = AmperePalette.PAGE_BACKGROUND_COLOR_LIGHT

    return base_style


@callback(
    [
        Output("stargazers-dropdown", "style"),
        Output("followers-dropdown", "style"),
    ],
    Input("color-mode-switch", "value"),
)
def update_network_dropdown_color(dark_mode: bool):
    if dark_mode:
        text_color = AmperePalette.BRAND_TEXT_COLOR_MUTED
        background_color = AmperePalette.PAGE_BACKGROUND_COLOR_DARK
    else:
        text_color = AmperePalette.PAGE_ACCENT_COLOR
        background_color = AmperePalette.PAGE_BACKGROUND_COLOR_LIGHT

    return (
        {"color": text_color, "backgroundColor": background_color},
        {"color": text_color, "backgroundColor": background_color},
    )


@callback(
    Output("navbar-collapse", "is_open"),
    [Input("navbar-toggler", "n_clicks")],
    [State("navbar-collapse", "is_open")],
)
def toggle_navbar_collapse(n_clicks, is_open):
    if n_clicks:
        return not is_open
    return is_open


@callback(
    Output("navbar-collapse", "is_open", allow_duplicate=True),
    Input("current-url", "pathname"),
    prevent_initial_call=True,
)
def close_navbar_on_navigate(_):
    return False


def layout(initial_background_color: str):
    navbar = dbc.Navbar(
        dbc.Container(
            [
                dbc.NavbarBrand("ampere", href="/", class_name="navbar-brand"),
                dbc.NavbarToggler(id="navbar-toggler", className="navbar-toggler"),
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
                                        id="stargazers-dropdown",
                                        children="stargazers",
                                        href="network-stargazers",
                                        style={
                                            "color": AmperePalette.PAGE_ACCENT_COLOR,
                                            "borderBottom": "1px solid white",
                                        },
                                    ),
                                    dbc.DropdownMenuItem(
                                        id="followers-dropdown",
                                        children="followers",
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
                    dbc.Switch(
                        id="color-mode-switch",
                        value=False,
                        persistence=True,
                        label="🌛",
                        input_style={"marginTop": "14px", "marginBottom": "0px"},
                        label_style={
                            "fontSize": "1.5em",
                            "marginTop": "4px",
                            "marginBottom": "0px",
                        },
                        class_name="color-mode-switch",
                    )
                ),
            ],
            fluid=True,
            class_name="navbar-container",
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
        style={
            "paddingLeft": "5%",
            "paddingRight": "5%",
            "paddingBottom": "3%",
            "backgroundColor": initial_background_color,
        },
    )


def main(env: str = "prod"):
    envs = {
        "prod": {"host": "0.0.0.0", "debug": False, "serve_locally": True},
        "dev": {"host": "127.0.0.1", "debug": True, "serve_locally": True},
    }

    app = dash.Dash(
        use_pages=True,
        external_stylesheets=["assets/css/bootstrap.min.css"],
        suppress_callback_exceptions=True,
        compress=True,
        serve_locally=envs[env]["serve_locally"],
    )

    initial_background_color = AmperePalette.PAGE_BACKGROUND_COLOR_LIGHT
    app.index_string = f"""
    <!DOCTYPE html>
    <html>
        <head>
            {{%metas%}}
            <title>{{%title%}}</title>
            {{%favicon%}}
            {{%css%}}
            <style>
                body {{
                    background-color: {initial_background_color};
                    margin: 0; /* Remove default margin */
                }}
            </style>
        </head>
        <body>
            {{%app_entry%}}
            <footer>
                {{%config%}}
                {{%scripts%}}
                {{%renderer%}}
            </footer>
        </body>
    </html>
    """
    server = app.server
    cache.init_app(server)

    app.layout = layout(initial_background_color)

    dash.register_page(summary.__name__, name="summary", path="/", layout=summary.layout)
    dash.register_page(downloads.__name__, name="downloads", layout=downloads.layout)
    dash.register_page(feed.__name__, name="feed", layout=feed.layout)
    dash.register_page(issues.__name__, name="issues", layout=issues.layout)

    dash.register_page(network_followers.__name__, layout=network_followers.layout)
    dash.register_page(network_stargazers.__name__, layout=network_stargazers.layout)

    dash.register_page(status.__name__, name="status", layout=status.layout)
    dash.register_page(about.__name__, name="about", layout=about.layout)

    return app, server, envs


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--env",
        type=str,
        default="prod",
        help="Environment to run the app in. Default is prod",
    )

    env = parser.parse_args().env
    app, server, envs = main(env)
    app.run(host=envs[env]["host"], debug=envs[env]["debug"])
_, server, _ = main()
