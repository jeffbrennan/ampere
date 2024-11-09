# placeholder for pypi downloads

import dash
from dash import dcc, html

dash.register_page(__name__, name="downloads",  top_nav=True, order=1)

layout = [
    # dummy input for reload on refresh
    dcc.Loading(
        id="loading-graph",
        type="default",
        children=[html.H1("coming soon!")],
    ),
]
