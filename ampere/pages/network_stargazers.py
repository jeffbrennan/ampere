import dash
import dash_bootstrap_components as dbc
from dash import html

from .side_bar import sidebar

dash.register_page(__name__, name="network", top_nav=True, order=1)


def layout(**kwargs):
    return dbc.Row(
        [dbc.Col(sidebar(), width=2), dbc.Col(html.Div("Stargazers"), width=10)]
    )
