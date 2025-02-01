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
    if dark_mode:
        text_color = AmperePalette.BRAND_TEXT_COLOR_DARK
        highlighted_text_color = AmperePalette.BRAND_TEXT_COLOR_LIGHT
        highlighted_background_color = AmperePalette.PAGE_BACKGROUND_COLOR_LIGHT
    else:
        text_color = AmperePalette.BRAND_TEXT_COLOR_LIGHT
        highlighted_text_color = AmperePalette.BRAND_TEXT_COLOR_DARK
        highlighted_background_color = AmperePalette.PAGE_BACKGROUND_COLOR_DARK

    pages = ["", "downloads", "feed", "issues", "network", "status", "about"]
    output_styles = [{"color": text_color} for _ in range(len(pages))]

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
    [
        Output("navbar", "color"),
        Output("stargazers-dropdown", "style"),
        Output("followers-dropdown", "style"),
    ],
    Input("color-mode-switch", "value"),
)
def update_navbar_color(dark_mode: bool):
    if dark_mode:
        text_color = AmperePalette.BRAND_TEXT_COLOR_DARK
        background_color = AmperePalette.PAGE_BACKGROUND_COLOR_DARK
    else:
        text_color = AmperePalette.BRAND_TEXT_COLOR_LIGHT
        background_color = AmperePalette.PAGE_BACKGROUND_COLOR_LIGHT

    dropdown_style = {
        "color": text_color,
        "backgroundColor": background_color,
        "borderBottom": f"2px solid {text_color}",
    }

    return (
        background_color,
        dropdown_style,
        dropdown_style,
    )


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
            style={
                "color": AmperePalette.PAGE_BACKGROUND_COLOR_LIGHT,
                "transform": "scale(1.3)",
            },
        ), True

    return html.I(
        className="fas fa-moon",
        style={
            "color": AmperePalette.PAGE_BACKGROUND_COLOR_DARK,
            "transform": "scale(1.3)",
        },
    ), False


def layout(initial_background_color: str):
    navbar = dbc.Navbar(
        dbc.Container(
            [
                dbc.NavbarBrand(
                    "ampere", href="/", class_name="navbar-brand", id="navbar-brand"
                ),
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
                        children=html.I(className="fas fa-sun"),
                        className="me-1",
                        color="link",
                    )
                ),
            ],
            fluid=True,
            class_name="navbar-container",
        ),
        id="navbar",
        dark=True,
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


def init_app(env: str = "prod"):
    serve_locally = {"dev": True, "prod": False}[env]

    app = dash.Dash(
        use_pages=True,
        external_stylesheets=["assets/css/bootstrap.min.css", dbc.icons.FONT_AWESOME],
        suppress_callback_exceptions=True,
        compress=True,
        serve_locally=serve_locally,
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
    app.run(host=envs[env]["host"], debug=envs[env]["debug"])


app, server = init_app()

if __name__ == "__main__":
    run_app(app)
