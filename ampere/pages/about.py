from dash import html
import dash

dash.register_page(__name__, name='about', top_nav=True, order=2)


layout = html.Div("About page content")
