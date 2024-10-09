import dash
from dash import html

dash.register_page(__name__, name="summary", path="/", top_nav=True, order=0)

layout = html.Div("Home page content")
