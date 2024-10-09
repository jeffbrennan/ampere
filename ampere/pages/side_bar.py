import dash
import dash_bootstrap_components as dbc
from dash import html


def clean_network_name(orig_name: str) -> str:
    return orig_name.replace("network followers", "followers").replace(
        "network", "stargazers"
    )


def sidebar():
    pages_sorted = sorted(
        dash.page_registry.values(),
        key=lambda page: clean_network_name(page["name"]),
        reverse=True,
    )
    return html.Div(
        dbc.Nav(
            [
                dbc.NavLink(
                    [
                        html.Div(
                            clean_network_name(page["name"]),
                            className="ms-2",
                        ),
                    ],
                    href=page["path"],
                    active="exact",
                )
                for page in pages_sorted
                if page["path"].startswith("/network")
            ],
            vertical=True,
            pills=True,
            className="bg-light",
        )
    )
