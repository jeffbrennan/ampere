import argparse
import os

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
from ampere.styling import AmperePalette, ScreenWidth, get_ampere_colors


@callback(
    [
        Output("navbar-brand", "style"),
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
    _, color = get_ampere_colors(dark_mode, contrast=False)
    highlighted_background_color, highlighted_text_color = get_ampere_colors(
        dark_mode, contrast=True
    )

    pages = ["", "downloads", "feed", "issues", "network", "status", "about"]
    output_styles = [{"color": color} for _ in range(len(pages))]

    if pathname in ["network"]:
        return output_styles

    current_page = pathname.removeprefix("/").split("-")[0]

    output_styles[pages.index(current_page)] = {
        "color": highlighted_text_color,
        "backgroundColor": highlighted_background_color,
        "borderRadius": "20px",
    }

    return output_styles


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


@callback(
    [
        Output("color-mode-switch", "children"),
        Output("color-mode-switch", "value"),
    ],
    Input("color-mode-switch", "n_clicks"),
    State("color-mode-switch", "children"),
)
def toggle_color_mode(n_clicks, _):
    is_dark = n_clicks % 2 == 1
    if is_dark:
        return html.I(
            className="fas fa-sun",
            style={"color": AmperePalette.PAGE_BACKGROUND_COLOR_LIGHT},
        ), True

    return html.I(
        className="fas fa-moon",
        style={"color": AmperePalette.PAGE_BACKGROUND_COLOR_DARK},
    ), False


@callback(
    [
        Output("ampere-page", "className"),
        Output("navbar", "className"),
    ],
    Input("color-mode-switch", "value"),
)
def toggle_page_color(dark_mode: bool):
    class_name = "dark-mode" if dark_mode else "light-mode"
    return class_name, class_name


def layout():
    navbar = dbc.Navbar(
        dbc.Container(
            [
                dbc.NavbarBrand(
                    "ampere", href="/", class_name="navbar-brand", id="navbar-brand"
                ),
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
                                    class_name="downloads-link",
                                )
                            ),
                            dbc.NavItem(
                                dbc.NavLink(id="feed-link", children="feed", href="feed")
                            ),
                            dbc.NavItem(
                                dbc.NavLink(
                                    id="issues-link", children="issues", href="issues"
                                )
                            ),
                            dbc.DropdownMenu(
                                children=[
                                    dbc.DropdownMenuItem(
                                        id="stargazers-dropdown",
                                        children="stargazers",
                                        href="network-stargazers",
                                    ),
                                    dbc.DropdownMenuItem(
                                        id="followers-dropdown",
                                        children="followers",
                                        href="network-followers",
                                    ),
                                ],
                                id="network-link",
                                nav=True,
                                in_navbar=True,
                                label="networks",
                            ),
                            dbc.NavItem(
                                dbc.NavLink(
                                    id="status-link", children="status", href="status"
                                )
                            ),
                            dbc.NavItem(
                                dbc.NavLink(
                                    id="about-link", children="about", href="about"
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
                    dbc.Button(
                        id="color-mode-switch",
                        n_clicks=0,
                        children=html.I(
                            className="fas fa-moon",
                            style={
                                "color": AmperePalette.PAGE_BACKGROUND_COLOR_DARK,
                            },
                        ),
                        color="link",
                    )
                ),
            ],
            fluid=True,
            id="navbar-container",
        ),
        id="navbar",
    )

    return dbc.Container(
        id="ampere-page",
        children=[
            navbar,
            html.Br(),
            dash_breakpoints.WindowBreakpoints(
                id="breakpoints",
                widthBreakpointThresholdsPx=[
                    768,
                    1200,
                    1920,
                    2560,
                ],
                widthBreakpointNames=[i.value for i in ScreenWidth],
            ),
            dash.page_container,
        ],
        fluid=True,
    )


def init_app(env: str = "prod"):
    serve_locally = {"dev": True, "prod": False}[env]

    app = dash.Dash(
        use_pages=True,
        external_stylesheets=["assets/css/bootstrap.min.css", dbc.icons.FONT_AWESOME],
        suppress_callback_exceptions=True,
        compress=True,
        serve_locally=serve_locally,
    )

    app.index_string = f"""
    <!DOCTYPE html>
    <html>
        <head>
            {{%metas%}}
            <title>{{%title%}}</title>
            {{%favicon%}}
            {{%css%}}
            <style>
                body, #navbar {{
                    background-color: {AmperePalette.PAGE_BACKGROUND_COLOR_LIGHT} !important;
                    margin: 0;
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

    app.layout = layout()

    dash.register_page(summary.__name__, name="summary", path="/", layout=summary.layout)
    dash.register_page(downloads.__name__, name="downloads", layout=downloads.layout)
    dash.register_page(feed.__name__, name="feed", layout=feed.layout)
    dash.register_page(issues.__name__, name="issues", layout=issues.layout)

    dash.register_page(network_followers.__name__, layout=network_followers.layout)
    dash.register_page(network_stargazers.__name__, layout=network_stargazers.layout)

    dash.register_page(status.__name__, name="status", layout=status.layout)
    dash.register_page(about.__name__, name="about", layout=about.layout)

    return app, server


def run_app(app):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--env",
        type=str,
        default="prod",
        help="Environment to run the app in. Default is prod",
    )
    envs = {
        "prod": {"host": "0.0.0.0", "debug": False},
        "dev": {"host": "127.0.0.1", "debug": True},
    }

    env = parser.parse_args().env
    os.environ["AMPERE_ENV"] = env

    app.run(host=envs[env]["host"], debug=envs[env]["debug"])


app, server = init_app()

if __name__ == "__main__":
    run_app(app)
